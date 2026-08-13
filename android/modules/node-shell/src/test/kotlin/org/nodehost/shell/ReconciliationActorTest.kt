package org.nodehost.shell

import android.content.Context
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.nodehost.core.DesiredRuntimeAcceptance
import org.nodehost.core.OperationContext
import org.nodehost.core.RuntimeBackend
import org.nodehost.core.RuntimeStep
import org.nodehost.core.StepOutcome
import org.nodehost.model.DesiredRuntimeState
import org.nodehost.model.OperationId
import org.nodehost.model.OperationRecord
import org.nodehost.model.OperationState
import org.nodehost.model.RuntimeId
import org.nodehost.model.RuntimeObservation
import org.nodehost.model.RuntimeSpec
import org.nodehost.model.VmProfileId
import org.nodehost.store.NodeHostDatabase
import org.nodehost.store.RoomOperationRepository
import org.nodehost.store.StepStatus
import org.nodehost.testsupport.FakeClock
import org.nodehost.testsupport.FakeFailurePoint
import org.nodehost.testsupport.FakeRuntimeBackend
import org.nodehost.testsupport.FakeRuntimeState
import org.robolectric.RobolectricTestRunner

@RunWith(RobolectricTestRunner::class)
class ReconciliationActorTest {
    private lateinit var database: NodeHostDatabase
    private lateinit var store: RoomOperationRepository
    private val clock = FakeClock(1_000)
    private val scopes = mutableListOf<CoroutineScope>()

    @Before fun setUp() {
        database = Room.inMemoryDatabaseBuilder(
            ApplicationProvider.getApplicationContext<Context>(), NodeHostDatabase::class.java,
        ).build()
        store = RoomOperationRepository(database, clock)
    }

    @After fun tearDown() {
        scopes.forEach(CoroutineScope::cancel)
        database.close()
    }

    @Test
    fun processDeathAfterEffectRecoversFromObservationWithoutRepeatingStart() = runBlocking {
        val operation = acceptDesired()
        val external = FakeRuntimeState()
        val firstRuntime = FakeRuntimeBackend(external).apply {
            failStepId = "qemu.start_process"
            failurePoint = FakeFailurePoint.AFTER_EFFECT_UNKNOWN_OUTCOME
        }
        runActor(firstRuntime, expectFailure = true)
        assertTrue(external.observation is RuntimeObservation.Starting)
        assertEquals(StepStatus.FAILED.name, store.steps(operation.id).last().status)

        val restartedRuntime = FakeRuntimeBackend(external)
        runActor(restartedRuntime)

        assertEquals(0, restartedRuntime.executed.count { it.second.id == "qemu.start_process" })
        assertEquals(RuntimeObservation.Running(RuntimeId.DEFAULT, 4242, true), external.observation)
        assertEquals(OperationState.SUCCEEDED, store.load(operation.id)?.state)
    }

    @Test
    fun retryableFailureSchedulesBoundedRetryWithoutSecondWake() = runBlocking {
        val operation = acceptDesired()
        val delegate = FakeRuntimeBackend()
        var failures = 0
        val runtime = object : RuntimeBackend {
            override suspend fun observe(id: RuntimeId) = delegate.observe(id)
            override suspend fun execute(context: OperationContext, step: RuntimeStep): StepOutcome {
                if (step == RuntimeStep.StartProcess && failures++ == 0) error("transient")
                return delegate.execute(context, step)
            }
        }
        val scope = newScope()
        val actor = ReconciliationActor(
            scope, store, runtime, retryBaseDelayMillis = 1, retryMaxDelayMillis = 2,
        )
        actor.wake(WakeReason.SERVICE_STARTED)
        withTimeout(5_000) {
            while (store.load(operation.id)?.state != OperationState.SUCCEEDED) kotlinx.coroutines.yield()
        }
        assertEquals(listOf(1, 2), store.steps(operation.id)
            .filter { it.stepId == RuntimeStep.StartProcess.id }.map { it.attempt })
        actor.stop()
    }

    @Test
    fun failedEffectIsRetriedWithANewBoundedAttemptAndStableJournal() = runBlocking {
        val operation = acceptDesired()
        val runtime = FakeRuntimeBackend().apply {
            failStepId = "qemu.start_process"
            failurePoint = FakeFailurePoint.BEFORE_EFFECT
        }
        runActor(runtime, expectFailure = true)
        runtime.failurePoint = FakeFailurePoint.NONE
        runActor(runtime)

        val starts = store.steps(operation.id).filter { it.stepId == "qemu.start_process" }
        assertEquals(listOf(1, 2), starts.map { it.attempt })
        assertEquals(listOf(StepStatus.FAILED.name, StepStatus.SUCCEEDED.name), starts.map { it.status })
    }

    @Test
    fun taskRemovalDoesNotRedefineDurableDesiredState() = runBlocking {
        acceptDesired()
        NodeSupervisorService().onTaskRemoved(null)
        assertEquals(DesiredRuntimeState.RUNNING, store.loadDesiredRuntime(RuntimeId.DEFAULT)?.desiredState)
    }

    @Test
    fun cancelBeforeEffectPreventsExecutionAndPreservesDesiredState() = runBlocking {
        val operation = acceptDesired()
        val executions = AtomicInteger()
        val runtime = object : RuntimeBackend {
            override suspend fun observe(id: RuntimeId): RuntimeObservation {
                store.cancelOperation(operation.id, setOf(OperationState.ACCEPTED, OperationState.PREFLIGHT, OperationState.STARTING_QEMU))
                return RuntimeObservation.Absent(id)
            }
            override suspend fun execute(context: OperationContext, step: RuntimeStep): StepOutcome {
                executions.incrementAndGet()
                return StepOutcome(true)
            }
        }

        runActor(runtime)

        assertEquals(0, executions.get())
        assertEquals(OperationState.CANCELLED, store.load(operation.id)?.state)
        assertEquals(DesiredRuntimeState.RUNNING, store.loadDesiredRuntime(RuntimeId.DEFAULT)?.desiredState)
    }

    @Test
    fun cancelDuringEffectStopsFollowingEffectsAndCannotBeOverwritten() = runBlocking {
        val operation = acceptDesired()
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val executions = AtomicInteger()
        val runtime = object : RuntimeBackend {
            override suspend fun observe(id: RuntimeId) = RuntimeObservation.Absent(id)
            override suspend fun execute(context: OperationContext, step: RuntimeStep): StepOutcome {
                executions.incrementAndGet()
                entered.complete(Unit)
                release.await()
                return StepOutcome(true)
            }
        }
        val completion = startActor(runtime)
        withTimeout(5_000) { entered.await() }
        store.cancelOperation(operation.id, setOf(OperationState.ACCEPTED, OperationState.PREFLIGHT, OperationState.STARTING_QEMU))
        release.complete(Unit)
        withTimeout(5_000) { completion.await() }

        assertEquals(1, executions.get())
        assertEquals(OperationState.CANCELLED, store.load(operation.id)?.state)
        assertEquals(StepStatus.STARTED.name, store.steps(operation.id).single().status)
    }

    @Test
    fun cancelImmediatelyBeforeSuccessUpdateWinsCompareAndSet() = runBlocking {
        val operation = acceptDesired()
        val runtime = object : RuntimeBackend {
            override suspend fun observe(id: RuntimeId) = RuntimeObservation.Absent(id)
            override suspend fun execute(context: OperationContext, step: RuntimeStep): StepOutcome {
                store.cancelOperation(operation.id, setOf(OperationState.ACCEPTED, OperationState.PREFLIGHT, OperationState.STARTING_QEMU))
                return StepOutcome(true)
            }
        }

        runActor(runtime)

        assertEquals(OperationState.CANCELLED, store.load(operation.id)?.state)
        assertEquals(StepStatus.STARTED.name, store.steps(operation.id).single().status)
    }

    @Test
    fun restartAfterCancelDoesNotReplayEffects() = runBlocking {
        val operation = acceptDesired()
        store.cancelOperation(operation.id, setOf(OperationState.ACCEPTED, OperationState.PREFLIGHT, OperationState.STARTING_QEMU))
        val runtime = FakeRuntimeBackend()

        runActor(runtime)

        assertTrue(runtime.executed.isEmpty())
        assertEquals(OperationState.CANCELLED, store.load(operation.id)?.state)
    }

    @Test
    fun unknownStartedIntentFailsPermanentWithoutReplayingEffect() = runBlocking {
        val operation = acceptDesired()
        store.beginStep(operation.id, "qemu.verify_profile")
        val runtime = FakeRuntimeBackend()

        runActor(runtime, expectFailure = true)

        assertTrue(runtime.executed.isEmpty())
        assertEquals(OperationState.FAILED_PERMANENT, store.load(operation.id)?.state)
        assertEquals("UNKNOWN_EFFECT_OUTCOME", store.load(operation.id)?.errorCode)
        assertEquals(StepStatus.FAILED.name, store.steps(operation.id).single().status)
    }

    @Test
    fun effectDeadlinePersistsExplicitRetryableFailure() = runBlocking {
        val operation = acceptDesired()
        val delegate = FakeRuntimeBackend()
        val blocked = object : RuntimeBackend {
            override suspend fun observe(id: RuntimeId) = delegate.observe(id)
            override suspend fun execute(context: OperationContext, step: RuntimeStep): StepOutcome {
                delay(10_000)
                return delegate.execute(context, step)
            }
        }
        val finished = CompletableDeferred<ReconciliationEvent>()
        val scope = newScope()
        val actor = ReconciliationActor(scope, store, blocked, effectTimeoutMillis = 25, events = {
            if (it is ReconciliationEvent.Failed) finished.complete(it)
        })
        actor.wake(WakeReason.RETRY)
        withTimeout(5_000) { finished.await() }
        actor.close()

        assertEquals("EFFECT_TIMEOUT", store.load(operation.id)?.errorCode)
        assertEquals(StepStatus.FAILED.name, store.steps(operation.id).single().status)
    }

    @Test
    fun guestReadyOldGenerationDoesNotConvergeRunningOperation() = runBlocking {
        val operation = acceptDesired()
        val executed = CopyOnWriteArrayList<String>()
        val runtime = object : RuntimeBackend {
            override suspend fun observe(id: RuntimeId) = RuntimeObservation.Running(
                id, processId = 42, guestReady = true, appliedGeneration = 2,
            )
            override suspend fun execute(context: OperationContext, step: RuntimeStep): StepOutcome {
                executed += step.id
                return StepOutcome(true)
            }
        }

        runActor(runtime)

        assertEquals(listOf(RuntimeStep.RequestShutdown.id), executed)
        assertEquals(OperationState.PREFLIGHT, store.load(operation.id)?.state)
    }

    @Test
    fun ignoredGracefulShutdownCannotSucceedAndDeadlineWakeForceStops() = runBlocking {
        val operation = acceptDesired(DesiredRuntimeState.STOPPED)
        var observation: RuntimeObservation = RuntimeObservation.Running(RuntimeId.DEFAULT, 42, true)
        val executed = CopyOnWriteArrayList<String>()
        val runtime = object : RuntimeBackend {
            override suspend fun observe(id: RuntimeId) = observation
            override suspend fun execute(context: OperationContext, step: RuntimeStep): StepOutcome {
                executed += step.id
                observation = when (step) {
                    RuntimeStep.RequestShutdown -> RuntimeObservation.Stopping(id = RuntimeId.DEFAULT, processId = 42, gracefulDeadlineExceeded = false)
                    RuntimeStep.ForceStop -> RuntimeObservation.Stopped(RuntimeId.DEFAULT, null)
                    else -> observation
                }
                return StepOutcome(true)
            }
        }

        runActor(runtime)
        assertEquals(OperationState.PREFLIGHT, store.load(operation.id)?.state)
        assertEquals(listOf("qemu.request_shutdown"), executed)

        observation = RuntimeObservation.Stopping(RuntimeId.DEFAULT, 42, gracefulDeadlineExceeded = true)
        runActor(runtime)
        assertEquals(listOf("qemu.request_shutdown", "qemu.force_stop"), executed)
        assertEquals(OperationState.SUCCEEDED, store.load(operation.id)?.state)
    }

    @Test
    fun normalLateExitCompletesStopWithoutForceStop() = runBlocking {
        val operation = acceptDesired(DesiredRuntimeState.STOPPED)
        var observation: RuntimeObservation = RuntimeObservation.Running(RuntimeId.DEFAULT, 42, true)
        val executed = CopyOnWriteArrayList<String>()
        val runtime = object : RuntimeBackend {
            override suspend fun observe(id: RuntimeId) = observation
            override suspend fun execute(context: OperationContext, step: RuntimeStep): StepOutcome {
                executed += step.id
                observation = RuntimeObservation.Stopping(RuntimeId.DEFAULT, 42, false)
                return StepOutcome(true)
            }
        }

        runActor(runtime)
        observation = RuntimeObservation.Stopped(RuntimeId.DEFAULT, null)
        runActor(runtime)

        assertEquals(listOf("qemu.request_shutdown"), executed)
        assertEquals(OperationState.SUCCEEDED, store.load(operation.id)?.state)
    }

    @Test
    fun spontaneousExitAfterSucceededCreatesDurableMaintenanceAndRestarts() = runBlocking {
        val userOperation = acceptDesired()
        val external = FakeRuntimeState()
        runActor(FakeRuntimeBackend(external))
        assertEquals(OperationState.SUCCEEDED, store.load(userOperation.id)?.state)

        external.observation = RuntimeObservation.Absent(RuntimeId.DEFAULT)
        val maintenanceRuntime = FakeRuntimeBackend(external)
        runActor(maintenanceRuntime)

        val maintenance = requireNotNull(store.operationForDesired(requireNotNull(store.loadDesiredRuntime(RuntimeId.DEFAULT))))
        assertTrue(store.isSystemReconciliation(maintenance.id))
        assertEquals(OperationState.SUCCEEDED, maintenance.state)
        assertEquals(1, maintenanceRuntime.executed.count { it.second == RuntimeStep.StartProcess })
        assertEquals(DesiredRuntimeState.RUNNING, store.loadDesiredRuntime(RuntimeId.DEFAULT)?.desiredState)
    }

    @Test
    fun processDeathDuringMaintenanceUsesNewAttemptWithoutReplayingUnknownEffect() = runBlocking {
        acceptDesired()
        val external = FakeRuntimeState()
        runActor(FakeRuntimeBackend(external))
        external.observation = RuntimeObservation.Absent(RuntimeId.DEFAULT)
        val desired = requireNotNull(store.loadDesiredRuntime(RuntimeId.DEFAULT))
        val interrupted = requireNotNull(store.beginSystemReconciliation(desired))
        store.beginStep(interrupted.id, RuntimeStep.StartProcess.id)

        val recoveryProbe = FakeRuntimeBackend(external)
        runActor(recoveryProbe, expectFailure = true)
        assertEquals(0, recoveryProbe.executed.count { it.second == RuntimeStep.StartProcess })
        assertEquals(OperationState.FAILED_PERMANENT, store.load(interrupted.id)?.state)

        val restartedRuntime = FakeRuntimeBackend(external)
        runActor(restartedRuntime)

        val maintenance = requireNotNull(store.operationForDesired(desired))
        assertTrue(maintenance.id.value.endsWith("-2"))
        assertEquals(OperationState.SUCCEEDED, maintenance.state)
        assertEquals(1, restartedRuntime.executed.count { it.second == RuntimeStep.StartProcess })
    }

    @Test
    fun repeatedExitWakesReuseOneMaintenanceAttempt() = runBlocking {
        acceptDesired()
        val external = FakeRuntimeState()
        runActor(FakeRuntimeBackend(external))
        external.observation = RuntimeObservation.Absent(RuntimeId.DEFAULT)
        val runtime = FakeRuntimeBackend(external)
        val events = CopyOnWriteArrayList<ReconciliationEvent>()
        val completed = CompletableDeferred<Unit>()
        val actor = ReconciliationActor(newScope(), store, runtime, events = {
            events += it
            if (it is ReconciliationEvent.Completed) completed.complete(Unit)
        })

        repeat(100) { actor.wake(WakeReason.RUNTIME_EVENT) }
        withTimeout(5_000) { completed.await() }
        actor.stop()

        val maintenance = requireNotNull(store.operationForDesired(requireNotNull(store.loadDesiredRuntime(RuntimeId.DEFAULT))))
        assertEquals("sys-reconcile-default-1-1", maintenance.id.value)
        assertEquals(1, runtime.executed.count { it.second == RuntimeStep.StartProcess })
        assertTrue(events.count { it is ReconciliationEvent.Completed } <= 2)
    }

    @Test
    fun conflatedWakeQueueSerializesRuntimeMutations() = runBlocking {
        acceptDesired()
        val runtime = FakeRuntimeBackend()
        val events = CopyOnWriteArrayList<ReconciliationEvent>()
        val completed = CompletableDeferred<Unit>()
        val scope = newScope()
        val actor = ReconciliationActor(scope, store, runtime, events = {
            events += it
            if (it is ReconciliationEvent.Completed) completed.complete(Unit)
        })
        repeat(100) { actor.wake(WakeReason.RUNTIME_EVENT) }
        withTimeout(5_000) { completed.await() }
        actor.close()

        assertEquals(1, runtime.executed.count { it.second.id == "qemu.start_process" })
        assertTrue(events.count { it is ReconciliationEvent.Completed } <= 2)
    }

    private suspend fun acceptDesired(
        desiredState: DesiredRuntimeState = DesiredRuntimeState.RUNNING,
    ): OperationRecord {
        val operation = OperationRecord(
            OperationId("op-001"), "idempotency-key-0001", "a".repeat(64), RuntimeId.DEFAULT, 1,
            OperationState.ACCEPTED, null,
        )
        val accepted = store.acceptDesiredRuntime(
            RuntimeSpec(
                generation = 1,
                desiredState = desiredState,
                profileId = VmProfileId("alpine-direct"),
                memoryMiB = 512,
                vcpus = 1,
                dataDiskGiB = 4,
            ),
            operation,
        )
        assertTrue(accepted is DesiredRuntimeAcceptance.Accepted)
        return operation
    }

    private suspend fun runActor(runtime: RuntimeBackend, expectFailure: Boolean = false) {
        val finished = startActor(runtime)
        val result = withTimeout(5_000) { finished.await() }
        assertEquals(expectFailure, result is ReconciliationEvent.Failed)
    }

    private fun startActor(runtime: RuntimeBackend): CompletableDeferred<ReconciliationEvent> {
        val finished = CompletableDeferred<ReconciliationEvent>()
        val actor = ReconciliationActor(newScope(), store, runtime, events = {
            if (it is ReconciliationEvent.Completed || it is ReconciliationEvent.Failed) finished.complete(it)
        })
        actor.wake(WakeReason.SERVICE_STARTED)
        return finished
    }

    private fun newScope() = CoroutineScope(SupervisorJob() + Dispatchers.Default).also(scopes::add)
}
