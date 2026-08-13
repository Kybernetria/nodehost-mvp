package org.nodehost.shell

import java.io.Closeable
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import org.nodehost.core.NodePlanner
import org.nodehost.core.OperationContext
import org.nodehost.core.RuntimeBackend
import org.nodehost.model.DesiredRuntimeState
import org.nodehost.model.OperationRecord
import org.nodehost.model.OperationState
import org.nodehost.model.RuntimeId
import org.nodehost.model.RuntimeObservation
import org.nodehost.store.RoomOperationRepository
import org.nodehost.store.StepStatus

/** The only owner of RuntimeBackend mutations. Its conflated mailbox has a fixed capacity of one. */
class ReconciliationActor(
    scope: CoroutineScope,
    private val store: RoomOperationRepository,
    private val runtime: RuntimeBackend,
    private val effectTimeoutMillis: Long = DEFAULT_EFFECT_TIMEOUT_MILLIS,
    private val retryBaseDelayMillis: Long = DEFAULT_RETRY_BASE_DELAY_MILLIS,
    private val retryMaxDelayMillis: Long = DEFAULT_RETRY_MAX_DELAY_MILLIS,
    private val events: (ReconciliationEvent) -> Unit = {},
) : Closeable {
    init {
        require(effectTimeoutMillis > 0)
        require(retryBaseDelayMillis > 0 && retryMaxDelayMillis >= retryBaseDelayMillis)
        require(retryMaxDelayMillis <= MAX_RETRY_DELAY_MILLIS)
    }

    private val actorScope = scope
    private val wakes = Channel<WakeReason>(Channel.CONFLATED)
    private var retryJob: Job? = null
    private val actorJob: Job = scope.launch {
        for (reason in wakes) {
            try {
                reconcile(reason)
            } catch (failure: Exception) {
                events(ReconciliationEvent.Failed(reason, failure.javaClass.simpleName))
            }
        }
    }

    /** Never blocks a lifecycle or command caller; duplicate wakes are deliberately coalesced. */
    fun wake(reason: WakeReason): Boolean = wakes.trySend(reason).isSuccess

    private suspend fun reconcile(reason: WakeReason) {
        events(ReconciliationEvent.Started(reason))
        val desired = store.loadDesiredRuntime(RuntimeId.DEFAULT)
        if (desired == null) {
            events(ReconciliationEvent.Idle(reason))
            return
        }
        var operation = store.operationForDesired(desired)
        if (operation == null) {
            events(ReconciliationEvent.Failed(reason, "MissingOperation"))
            return
        }

        // Cancellation disposes only its user operation. It must never be converted into an
        // internal retry, because doing so would replay the cancelled operation's effects.
        if (operation.blocksEffects &&
            !(operation.state == OperationState.SUCCEEDED || store.isSystemReconciliation(operation.id))
        ) {
            events(ReconciliationEvent.Completed(reason, 0))
            return
        }

        var observed = observeAndRecord(desired.id)
        if (operation.blocksEffects) {
            if (operation.state == OperationState.SUCCEEDED && desired.isConverged(observed)) {
                events(ReconciliationEvent.Completed(reason, 0))
                return
            }
            operation = store.beginSystemReconciliation(desired)
            if (operation == null || operation.blocksEffects) {
                events(ReconciliationEvent.Failed(reason, "ReconciliationAttemptLimit"))
                return
            }
        }

        val plan = NodePlanner.plan(desired, observed)
        var executedSteps = 0
        for (step in plan.steps) {
            // Re-read at every boundary. beginStep repeats the check transactionally with intent creation.
            if (store.load(operation.id)?.blocksEffects != false) break
            val begun = store.beginStep(operation.id, step.id) ?: break
            val intent = begun.intent
            if (intent.status == StepStatus.SUCCEEDED.name) continue
            if (begun.recovered) {
                store.failUnknownStartedStep(intent)
                events(ReconciliationEvent.Failed(reason, "UnknownStartedIntent"))
                return
            }
            // Cancellation may commit after intent creation but before the external call.
            if (store.load(operation.id)?.blocksEffects != false) break
            try {
                val outcome = withTimeout(effectTimeoutMillis) {
                    runtime.execute(OperationContext(operation.id, step.id, intent.attempt), step)
                }
                if (!store.completeStep(intent, outcome)) break
                executedSteps++
            } catch (failure: Exception) {
                val failed = store.failStep(
                    intent,
                    if (failure is kotlinx.coroutines.TimeoutCancellationException) "EFFECT_TIMEOUT" else "EFFECT_FAILED",
                )
                if (failed) {
                    scheduleRetry(intent.attempt)
                    throw failure
                }
                break
            }
        }
        if (store.load(operation.id)?.blocksEffects == false) {
            observed = observeAndRecord(desired.id)
            // A Stopping observation intentionally has no graceful step before its monotonic
            // deadline, but it is not convergence and therefore cannot complete a STOP operation.
            if (desired.isConverged(observed) && store.load(operation.id)?.blocksEffects == false) {
                store.markSucceeded(operation.id)
            }
        }
        events(ReconciliationEvent.Completed(reason, executedSteps))
    }

    private suspend fun observeAndRecord(id: RuntimeId): RuntimeObservation {
        val observed = withTimeout(effectTimeoutMillis) { runtime.observe(id) }
        store.recordObservation(observed)
        return observed
    }

    private fun scheduleRetry(attempt: Int) {
        if (attempt >= RoomOperationRepository.MAX_STEP_ATTEMPTS) return
        retryJob?.cancel()
        val exponent = (attempt - 1).coerceIn(0, 30)
        val multiplier = 1L shl exponent
        val delayMillis = if (retryBaseDelayMillis > retryMaxDelayMillis / multiplier) {
            retryMaxDelayMillis
        } else {
            retryBaseDelayMillis * multiplier
        }
        retryJob = actorScope.launch {
            delay(delayMillis)
            wake(WakeReason.RETRY)
        }
    }

    suspend fun stop() {
        retryJob?.cancel()
        wakes.close()
        actorJob.join()
    }

    override fun close() {
        wakes.close()
    }

    companion object {
        const val DEFAULT_EFFECT_TIMEOUT_MILLIS = 30_000L
        const val DEFAULT_RETRY_BASE_DELAY_MILLIS = 250L
        const val DEFAULT_RETRY_MAX_DELAY_MILLIS = 30_000L
        const val MAX_RETRY_DELAY_MILLIS = 5 * 60_000L
    }
}

private fun org.nodehost.model.RuntimeSpec.isConverged(observed: RuntimeObservation): Boolean =
    when (desiredState) {
        DesiredRuntimeState.RUNNING -> observed is RuntimeObservation.Running &&
            observed.guestReady &&
            (observed.appliedGeneration == null || observed.appliedGeneration == generation)
        DesiredRuntimeState.STOPPED -> observed is RuntimeObservation.Stopped || observed is RuntimeObservation.Absent
        DesiredRuntimeState.ABSENT -> observed is RuntimeObservation.Absent
    }

private val OperationRecord.blocksEffects: Boolean
    get() = state.terminal || state == OperationState.CANCELLING

enum class WakeReason { SERVICE_STARTED, DESIRED_STATE_CHANGED, RUNTIME_EVENT, RETRY }

sealed interface ReconciliationEvent {
    val reason: WakeReason
    data class Started(override val reason: WakeReason) : ReconciliationEvent
    data class Idle(override val reason: WakeReason) : ReconciliationEvent
    data class Completed(override val reason: WakeReason, val plannedSteps: Int) : ReconciliationEvent
    data class Failed(override val reason: WakeReason, val classification: String) : ReconciliationEvent
}
