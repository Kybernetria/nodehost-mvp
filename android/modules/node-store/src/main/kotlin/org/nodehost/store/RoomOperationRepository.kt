package org.nodehost.store

import androidx.room.withTransaction
import java.security.MessageDigest
import org.nodehost.core.Clock
import org.nodehost.core.DesiredRuntimeAcceptance
import org.nodehost.core.OperationRepository
import org.nodehost.core.RuntimeStep
import org.nodehost.core.StepOutcome
import org.nodehost.model.DesiredRuntimeState
import org.nodehost.model.GenerationDecision
import org.nodehost.model.OperationId
import org.nodehost.model.OperationRecord
import org.nodehost.model.OperationState
import org.nodehost.model.OperationTransitions
import org.nodehost.model.RuntimeGenerationRules
import org.nodehost.model.RuntimeId
import org.nodehost.model.RuntimeObservation
import org.nodehost.model.RuntimeSpec
import org.nodehost.model.VmProfileId

/** Room's transaction is the serialization boundary for desired generations and idempotency keys. */
class RoomOperationRepository(
    private val database: NodeHostDatabase,
    private val clock: Clock,
) : OperationRepository {
    private val dao get() = database.dao()

    override suspend fun load(id: OperationId): OperationRecord? = dao.operation(id.value)?.toModel()

    /** Durable lookup used before reconstructing a DELETE request after a lost response. */
    suspend fun operationForIdempotencyKey(key: String): OperationRecord? = dao.operationByKey(key)?.toModel()

    override suspend fun save(record: OperationRecord) {
        database.withTransaction {
            val existing = checkNotNull(dao.operation(record.id.value)) { "operation does not exist" }
            check(existing.state !in EFFECT_BLOCKING_STATE_NAMES) { "terminal or cancelling operation cannot be overwritten" }
            check(compareAndSet(existing, record)) { "operation changed concurrently" }
        }
    }

    /** Atomically applies [updated] only while the durable operation still has [expected]'s state. */
    suspend fun compareAndSetOperation(expected: OperationRecord, updated: OperationRecord): Boolean = database.withTransaction {
        require(expected.id == updated.id) { "operation id cannot change" }
        compareAndSet(checkNotNull(dao.operation(expected.id.value)), updated, expected.state.name)
    }

    /** Cancellation never changes desired runtime state; only the target operation is disposed. */
    suspend fun cancelOperation(id: OperationId, cancellableStates: Set<OperationState>): OperationRecord = database.withTransaction {
        val stored = checkNotNull(dao.operation(id.value)) { "operation not found" }
        if (stored.state in EFFECT_BLOCKING_STATE_NAMES) return@withTransaction stored.toModel()
        val current = stored.toModel()
        require(current.state in cancellableStates) { "operation cannot be cancelled at its current step" }
        val cancelling = current.transitionTo(OperationState.CANCELLING)
        check(compareAndSet(stored, cancelling)) { "operation changed concurrently" }
        val cancelled = cancelling.transitionTo(OperationState.CANCELLED, stepId = null)
        check(compareAndSet(stored, cancelled, OperationState.CANCELLING.name)) { "cancellation disposition failed" }
        cancelled
    }

    override suspend fun loadDesiredRuntime(id: RuntimeId): RuntimeSpec? = dao.desired(id.value)?.toModel()

    override suspend fun acceptDesiredRuntime(
        spec: RuntimeSpec,
        operation: OperationRecord,
    ): DesiredRuntimeAcceptance = database.withTransaction {
        dao.operationByKey(operation.idempotencyKey)?.let { existing ->
            return@withTransaction if (existing.requestDigest == operation.requestDigest) {
                DesiredRuntimeAcceptance.Replay(existing.toModel())
            } else {
                DesiredRuntimeAcceptance.IdempotencyConflict
            }
        }

        require(dao.operationCount() < MAX_RETAINED_OPERATIONS) { "operation journal capacity exceeded" }
        val current = dao.desired(spec.id.value)?.toModel()
        val decision = RuntimeGenerationRules.decide(current, spec)
        if (decision !in setOf(GenerationDecision.INITIAL, GenerationDecision.ADVANCE)) {
            return@withTransaction DesiredRuntimeAcceptance.GenerationRejected(decision)
        }

        if (current == null) dao.insertDesired(spec.toEntity()) else dao.updateDesired(spec.toEntity())
        val now = clock.epochMillis()
        dao.insertOperation(operation.toEntity(now, now))
        DesiredRuntimeAcceptance.Accepted
    }

    suspend fun operationForDesired(spec: RuntimeSpec): OperationRecord? {
        latestSystemReconciliation(spec)?.let { return it }
        return dao.operationForGeneration(spec.id.value, spec.generation)?.toModel()
    }

    /**
     * Creates or reuses a durable reconciliation cycle without changing desired state.
     *
     * The fixed slot set bounds history without bounding lifetime recovery. Once all slots are
     * occupied, the oldest terminal slot is compacted inside this transaction. An active slot is
     * always reused, so concurrent/repeated runtime wakes remain idempotent.
     */
    suspend fun beginSystemReconciliation(spec: RuntimeSpec): OperationRecord? = database.withTransaction {
        val occupiedSlots = (1..MAX_RETAINED_SYSTEM_RECONCILIATIONS_PER_GENERATION).mapNotNull { slot ->
            dao.operation(systemReconciliationId(spec, slot))?.let { slot to it }
        }
        occupiedSlots.firstOrNull { (_, operation) ->
            !OperationState.valueOf(operation.state).terminal
        }?.let { return@withTransaction it.second.toModel() }

        val occupiedSlotNumbers = occupiedSlots.mapTo(mutableSetOf()) { it.first }
        val availableSlot = (1..MAX_RETAINED_SYSTEM_RECONCILIATIONS_PER_GENERATION)
            .firstOrNull { it !in occupiedSlotNumbers }
        val slot = if (availableSlot != null) {
            require(dao.operationCount() < MAX_RETAINED_OPERATIONS) { "operation journal capacity exceeded" }
            availableSlot
        } else {
            val (_, oldestTerminal) = occupiedSlots.minWith(
                compareBy<Pair<Int, OperationEntity>>(
                    { it.second.createdAtEpochMillis },
                    { it.second.updatedAtEpochMillis },
                    { it.first },
                ),
            )
            dao.deleteSteps(oldestTerminal.id)
            check(dao.deleteTerminalOperation(oldestTerminal.id, oldestTerminal.state) == 1) {
                "terminal reconciliation slot changed concurrently"
            }
            occupiedSlots.first { it.second.id == oldestTerminal.id }.first
        }

        val now = clock.epochMillis()
        val id = systemReconciliationId(spec, slot)
        val operation = OperationRecord(
            id = OperationId(id),
            idempotencyKey = "system-reconcile:${spec.id.value}:${spec.generation}:$slot",
            requestDigest = systemReconciliationDigest(spec),
            runtimeId = spec.id,
            desiredGeneration = spec.generation,
            state = OperationState.ACCEPTED,
            currentStepId = null,
        )
        dao.insertOperation(operation.toEntity(now, now))
        operation
    }

    fun isSystemReconciliation(id: OperationId): Boolean = id.value.startsWith(SYSTEM_RECONCILIATION_PREFIX)

    suspend fun current(id: RuntimeId): RuntimeObservation? = dao.current(id.value)?.toModel()

    suspend fun recordObservation(observation: RuntimeObservation) {
        dao.putCurrent(observation.toEntity(clock.epochMillis()))
    }

    /** Persists an inspectable intent before an effect, unless cancellation or a terminal state won the race. */
    suspend fun beginStep(operationId: OperationId, stepId: String): BeginStepResult? = database.withTransaction {
        val stored = checkNotNull(dao.operation(operationId.value))
        if (stored.state in EFFECT_BLOCKING_STATE_NAMES) return@withTransaction null
        val latest = dao.latestStep(operationId.value, stepId)
        if (latest?.status == StepStatus.STARTED.name) return@withTransaction BeginStepResult(latest, recovered = true)
        if (latest?.status == StepStatus.SUCCEEDED.name) return@withTransaction BeginStepResult(latest, recovered = false)
        val attempt = (latest?.attempt ?: 0) + 1
        require(attempt <= MAX_STEP_ATTEMPTS) { "step attempt limit exceeded" }
        val now = clock.epochMillis()
        val intent = OperationStepEntity(
            operationId = operationId.value,
            stepId = stepId,
            attempt = attempt,
            status = StepStatus.STARTED.name,
            startedAtEpochMillis = now,
            finishedAtEpochMillis = null,
            changed = null,
            resultDetail = null,
            errorCode = null,
        )
        val currentState = OperationState.valueOf(stored.state)
        val proposedState = lifecycleStateForStep(stepId) ?: currentState
        val nextState = when {
            proposedState == currentState -> currentState
            OperationTransitions.isAllowed(currentState, proposedState) -> proposedState
            currentState in LIFECYCLE_STATES && proposedState in LIFECYCLE_STATES &&
                LIFECYCLE_STATES.indexOf(currentState) > LIFECYCLE_STATES.indexOf(proposedState) -> currentState
            else -> OperationTransitions.requireAllowed(currentState, proposedState)
                .let { proposedState }
        }
        dao.insertStep(intent)
        check(dao.compareAndSetOperation(
            stored.id, stored.state, nextState.name, stepId,
            if (nextState == currentState) stored.errorCode else null, now,
        ) == 1) { "operation changed concurrently" }
        BeginStepResult(intent, recovered = false)
    }

    suspend fun completeStep(intent: OperationStepEntity, outcome: StepOutcome): Boolean = database.withTransaction {
        require(intent.status == StepStatus.STARTED.name)
        val stored = checkNotNull(dao.operation(intent.operationId))
        if (stored.state in EFFECT_BLOCKING_STATE_NAMES) return@withTransaction false
        dao.completeStartedStep(
            intent.operationId, intent.stepId, intent.attempt, StepStatus.SUCCEEDED.name,
            clock.epochMillis(), outcome.changed, outcome.observationHint?.take(MAX_RESULT_DETAIL_CHARS), null,
        ) == 1
    }

    suspend fun failStep(intent: OperationStepEntity, errorCode: String): Boolean = database.withTransaction {
        require(ERROR_CODE.matches(errorCode))
        val stored = checkNotNull(dao.operation(intent.operationId))
        if (stored.state in EFFECT_BLOCKING_STATE_NAMES) return@withTransaction false
        val now = clock.epochMillis()
        check(dao.completeStartedStep(
            intent.operationId, intent.stepId, intent.attempt, StepStatus.FAILED.name,
            now, null, null, errorCode,
        ) == 1) { "step changed concurrently" }
        val nextState = if (intent.attempt >= MAX_STEP_ATTEMPTS) {
            OperationState.FAILED_PERMANENT
        } else {
            OperationState.FAILED_RETRYABLE
        }
        dao.compareAndSetOperation(
            stored.id, stored.state, nextState.name,
            stored.currentStepId, errorCode, now,
        ) == 1
    }

    /** A recovered STARTED intent has an unknowable outcome and is never replayed. */
    suspend fun failUnknownStartedStep(intent: OperationStepEntity): Boolean = database.withTransaction {
        val stored = checkNotNull(dao.operation(intent.operationId))
        if (stored.state in EFFECT_BLOCKING_STATE_NAMES) return@withTransaction false
        val now = clock.epochMillis()
        check(dao.completeStartedStep(
            intent.operationId, intent.stepId, intent.attempt, StepStatus.FAILED.name,
            now, null, null, UNKNOWN_EFFECT_OUTCOME,
        ) == 1) { "step changed concurrently" }
        dao.compareAndSetOperation(
            stored.id, stored.state, OperationState.FAILED_PERMANENT.name,
            stored.currentStepId, UNKNOWN_EFFECT_OUTCOME, now,
        ) == 1
    }

    suspend fun markSucceeded(operationId: OperationId): Boolean = database.withTransaction {
        val stored = checkNotNull(dao.operation(operationId.value))
        if (stored.state in EFFECT_BLOCKING_STATE_NAMES) return@withTransaction false
        dao.compareAndSetOperation(
            stored.id, stored.state, OperationState.SUCCEEDED.name, null, null, clock.epochMillis(),
        ) == 1
    }

    suspend fun steps(operationId: OperationId): List<OperationStepEntity> = dao.steps(operationId.value)

    private fun lifecycleStateForStep(stepId: String): OperationState? = when (stepId) {
        RuntimeStep.VerifyProfile.id, RuntimeStep.RequestShutdown.id,
        RuntimeStep.ForceStop.id, RuntimeStep.RemoveSystem.id -> OperationState.PREFLIGHT
        RuntimeStep.ResolveArtifacts.id -> OperationState.FETCHING
        RuntimeStep.PrepareDisks.id -> OperationState.PREPARING_DISKS
        RuntimeStep.PrepareBoot.id -> OperationState.PREPARING_BOOT
        RuntimeStep.StartProcess.id -> OperationState.STARTING_QEMU
        RuntimeStep.WaitForQmp.id -> OperationState.WAITING_FOR_QMP
        RuntimeStep.WaitForGuest.id -> OperationState.BOOTSTRAPPING_GUEST
        else -> null
    }

    private suspend fun latestSystemReconciliation(spec: RuntimeSpec): OperationRecord? {
        val slots = (1..MAX_RETAINED_SYSTEM_RECONCILIATIONS_PER_GENERATION).mapNotNull { slot ->
            dao.operation(systemReconciliationId(spec, slot))
        }
        return slots.firstOrNull { !OperationState.valueOf(it.state).terminal }?.toModel()
            ?: slots.maxWithOrNull(
                compareBy<OperationEntity>(
                    { it.createdAtEpochMillis },
                    { it.updatedAtEpochMillis },
                    { it.id },
                ),
            )?.toModel()
    }

    private fun systemReconciliationId(spec: RuntimeSpec, attempt: Int) =
        "$SYSTEM_RECONCILIATION_PREFIX${spec.id.value}-${spec.generation}-$attempt"

    private fun systemReconciliationDigest(spec: RuntimeSpec): String {
        val canonical = listOf(
            spec.id.value, spec.generation, spec.desiredState.name, spec.profileId.value,
            spec.memoryMiB, spec.vcpus, spec.dataDiskGiB, spec.preserveDataOnDelete,
        ).joinToString("|")
        return MessageDigest.getInstance("SHA-256")
            .digest(canonical.toByteArray(Charsets.UTF_8))
            .joinToString("") { "%02x".format(it) }
    }

    private suspend fun compareAndSet(
        stored: OperationEntity,
        updated: OperationRecord,
        expectedState: String = stored.state,
    ): Boolean = dao.compareAndSetOperation(
        stored.id, expectedState, updated.state.name, updated.currentStepId, updated.errorCode, clock.epochMillis(),
    ) == 1

    companion object {
        const val MAX_STEP_ATTEMPTS = 100
        const val MAX_RETAINED_SYSTEM_RECONCILIATIONS_PER_GENERATION = 32
        const val MAX_RETAINED_OPERATIONS = 10_000L
        private const val SYSTEM_RECONCILIATION_PREFIX = "sys-reconcile-"
        private const val MAX_RESULT_DETAIL_CHARS = 512
        private const val UNKNOWN_EFFECT_OUTCOME = "UNKNOWN_EFFECT_OUTCOME"
        private val ERROR_CODE = Regex("[A-Z][A-Z0-9_]{0,63}")
        private val LIFECYCLE_STATES = listOf(
            OperationState.PREFLIGHT, OperationState.FETCHING, OperationState.VERIFYING,
            OperationState.PREPARING_DISKS, OperationState.PREPARING_BOOT,
            OperationState.STARTING_QEMU, OperationState.WAITING_FOR_QMP,
            OperationState.BOOTSTRAPPING_GUEST, OperationState.WAITING_FOR_SSH,
            OperationState.WAITING_FOR_GUEST_MESH,
        )
        private val EFFECT_BLOCKING_STATE_NAMES = (
            OperationState.entries.filter { it.terminal } + OperationState.CANCELLING
        ).mapTo(mutableSetOf()) { it.name }
    }
}

data class BeginStepResult(val intent: OperationStepEntity, val recovered: Boolean)

enum class StepStatus { STARTED, SUCCEEDED, FAILED }

private fun RuntimeSpec.toEntity() = DesiredRuntimeEntity(
    id.value, generation, desiredState.name, profileId.value, memoryMiB, vcpus, dataDiskGiB, preserveDataOnDelete,
)

private fun DesiredRuntimeEntity.toModel() = RuntimeSpec(
    RuntimeId(runtimeId), generation, DesiredRuntimeState.valueOf(state), VmProfileId(profileId),
    memoryMiB, vcpus, dataDiskGiB, preserveDataOnDelete,
)

private fun OperationRecord.toEntity(createdAt: Long, updatedAt: Long) = OperationEntity(
    id.value, idempotencyKey, requestDigest, runtimeId?.value, desiredGeneration, state.name,
    currentStepId, errorCode, createdAt, updatedAt,
)

private fun OperationEntity.toModel() = OperationRecord(
    OperationId(id), idempotencyKey, requestDigest, runtimeId?.let(::RuntimeId), desiredGeneration,
    OperationState.valueOf(state), currentStepId, errorCode,
)

private fun RuntimeObservation.toEntity(observedAt: Long): CurrentRuntimeEntity = when (this) {
    is RuntimeObservation.Absent -> CurrentRuntimeEntity(id.value, "ABSENT", null, null, null, null, null, observedAt)
    is RuntimeObservation.Stopped -> CurrentRuntimeEntity(id.value, "STOPPED", profileId?.value, null, null, null, null, observedAt)
    is RuntimeObservation.Starting -> CurrentRuntimeEntity(id.value, "STARTING", null, processId, null, null, null, observedAt)
    is RuntimeObservation.Running -> CurrentRuntimeEntity(id.value, "RUNNING", null, processId, guestReady, null, null, observedAt)
    is RuntimeObservation.Stopping -> CurrentRuntimeEntity(id.value, "STOPPING", null, processId, null, gracefulDeadlineExceeded, null, observedAt)
    is RuntimeObservation.Failed -> CurrentRuntimeEntity(id.value, "FAILED", null, null, null, null, "$code:${if (retryable) 1 else 0}", observedAt)
    is RuntimeObservation.Unknown -> CurrentRuntimeEntity(id.value, "UNKNOWN", null, null, null, null, reason.take(512), observedAt)
}

private fun CurrentRuntimeEntity.toModel(): RuntimeObservation = when (kind) {
    "ABSENT" -> RuntimeObservation.Absent(RuntimeId(runtimeId))
    "STOPPED" -> RuntimeObservation.Stopped(RuntimeId(runtimeId), profileId?.let(::VmProfileId))
    "STARTING" -> RuntimeObservation.Starting(RuntimeId(runtimeId), processId)
    "RUNNING" -> RuntimeObservation.Running(RuntimeId(runtimeId), processId, guestReady == true)
    "STOPPING" -> RuntimeObservation.Stopping(RuntimeId(runtimeId), processId, gracefulDeadlineExceeded == true)
    "FAILED" -> RuntimeObservation.Failed(RuntimeId(runtimeId), detail.orEmpty().substringBefore(':'), detail?.endsWith(":1") == true)
    else -> RuntimeObservation.Unknown(RuntimeId(runtimeId), detail ?: "unrecognized durable observation")
}
