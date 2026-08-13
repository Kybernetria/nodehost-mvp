package org.nodehost.store

import android.content.Context
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.nodehost.core.DesiredRuntimeAcceptance
import org.nodehost.core.StepOutcome
import org.nodehost.model.DesiredRuntimeState
import org.nodehost.model.OperationId
import org.nodehost.model.OperationRecord
import org.nodehost.model.OperationState
import org.nodehost.model.RuntimeId
import org.nodehost.model.RuntimeObservation
import org.nodehost.model.RuntimeSpec
import org.nodehost.model.VmProfileId
import org.robolectric.RobolectricTestRunner

@RunWith(RobolectricTestRunner::class)
class RoomOperationRepositoryTest {
    private lateinit var database: NodeHostDatabase
    private lateinit var repository: RoomOperationRepository
    private val clock = MutableClock(100)

    @Before
    fun setUp() {
        database = Room.inMemoryDatabaseBuilder(
            ApplicationProvider.getApplicationContext<Context>(),
            NodeHostDatabase::class.java,
        ).build()
        repository = RoomOperationRepository(database, clock)
    }

    @After fun tearDown() = database.close()

    @Test
    fun desiredGenerationAndOperationCommitAtomicallyAndRejectStaleOrConflict() = runBlocking {
        assertTrue(repository.acceptDesiredRuntime(spec(2), operation("op-200", "key-000000000002", 2)) is DesiredRuntimeAcceptance.Accepted)
        assertTrue(repository.acceptDesiredRuntime(spec(1), operation("op-100", "key-000000000001", 1)) is DesiredRuntimeAcceptance.GenerationRejected)
        assertTrue(repository.acceptDesiredRuntime(spec(2).copy(memoryMiB = 2048), operation("op-201", "key-000000000003", 2)) is DesiredRuntimeAcceptance.GenerationRejected)
        assertEquals(2L, repository.loadDesiredRuntime(RuntimeId.DEFAULT)?.generation)
        assertNull(repository.load(OperationId("op-100")))
    }

    @Test
    fun idempotencyReplayAndConflictAreDurableAndConcurrentSafe() = runBlocking {
        val sequence = AtomicInteger()
        val results = List(16) {
            async(Dispatchers.Default) {
                repository.acceptDesiredRuntime(
                    spec(1),
                    operation("op-${100 + sequence.incrementAndGet()}", "same-key-00000001", 1),
                )
            }
        }.awaitAll()

        assertEquals(1, results.count { it is DesiredRuntimeAcceptance.Accepted })
        assertEquals(15, results.count { it is DesiredRuntimeAcceptance.Replay })
        assertTrue(
            repository.acceptDesiredRuntime(spec(2), operation("op-999", "same-key-00000001", 2, digest = "b".repeat(64))) is DesiredRuntimeAcceptance.IdempotencyConflict,
        )
    }

    @Test
    fun intentAndResultJournalBracketEffectAndStartedIntentSurvivesRestart() = runBlocking {
        val operation = operation("op-300", "key-000000000300", 1)
        repository.acceptDesiredRuntime(spec(1), operation)
        val intent = requireNotNull(repository.beginStep(operation.id, "qemu.start_process")).intent
        assertEquals(StepStatus.STARTED.name, RoomOperationRepository(database, clock).steps(operation.id).single().status)

        clock.now = 200
        RoomOperationRepository(database, clock).completeStep(intent, StepOutcome(true, "started"))
        val result = repository.steps(operation.id).single()
        assertEquals(StepStatus.SUCCEEDED.name, result.status)
        assertEquals(200L, result.finishedAtEpochMillis)
        assertEquals("started", result.resultDetail)
    }

    @Test
    fun cancellationIsAtomicPreservesDesiredAndWinsAgainstStaleSuccessOrFailure() = runBlocking {
        val operation = operation("op-400", "key-000000000400", 1)
        repository.acceptDesiredRuntime(spec(1), operation)
        val intent = requireNotNull(repository.beginStep(operation.id, "qemu.start_process")).intent

        assertEquals(
            OperationState.CANCELLED,
            repository.cancelOperation(operation.id, setOf(OperationState.STARTING_QEMU)).state,
        )
        assertEquals(DesiredRuntimeState.RUNNING, repository.loadDesiredRuntime(RuntimeId.DEFAULT)?.desiredState)
        assertEquals(false, repository.completeStep(intent, StepOutcome(true, "started")))
        assertEquals(false, repository.failStep(intent, "EFFECT_FAILED"))
        assertEquals(false, repository.markSucceeded(operation.id))
        assertEquals(OperationState.CANCELLED, repository.load(operation.id)?.state)
        assertEquals(StepStatus.STARTED.name, repository.steps(operation.id).single().status)
    }

    @Test
    fun systemReconciliationAttemptHasStableIdAndConcurrentWakeDeduplication() = runBlocking {
        val userOperation = operation("op-500", "key-000000000500", 1)
        repository.acceptDesiredRuntime(spec(1), userOperation)
        repository.markSucceeded(userOperation.id)

        val attempts = List(16) {
            async(Dispatchers.Default) { repository.beginSystemReconciliation(spec(1)) }
        }.awaitAll()

        assertEquals(setOf("sys-reconcile-default-1-1"), attempts.map { it?.id?.value }.toSet())
        assertEquals(OperationState.ACCEPTED, repository.operationForDesired(spec(1))?.state)
        assertEquals(DesiredRuntimeState.RUNNING, repository.loadDesiredRuntime(RuntimeId.DEFAULT)?.desiredState)
    }

    @Test
    fun completedDriftCyclesRecoverForeverWithBoundedDurableHistory() = runBlocking {
        val desired = spec(1)
        val userOperation = operation("op-600", "key-000000000600", 1)
        repository.acceptDesiredRuntime(desired, userOperation)
        repository.markSucceeded(userOperation.id)

        repeat(65) { cycle ->
            clock.now = 1_000L + cycle
            val concurrentWakes = List(8) {
                async(Dispatchers.Default) { repository.beginSystemReconciliation(desired) }
            }.awaitAll()
            val attempt = requireNotNull(concurrentWakes.first())

            assertEquals(setOf(attempt.id), concurrentWakes.map { it?.id }.toSet())
            assertEquals(
                cycle % RoomOperationRepository.MAX_RETAINED_SYSTEM_RECONCILIATIONS_PER_GENERATION + 1,
                attempt.id.value.substringAfterLast('-').toInt(),
            )
            assertEquals(OperationState.ACCEPTED, attempt.state)

            val intent = requireNotNull(repository.beginStep(attempt.id, "qemu.start_process")).intent
            assertTrue(repository.completeStep(intent, StepOutcome(true, "recovered")))
            assertTrue(repository.markSucceeded(attempt.id))

            repository = RoomOperationRepository(database, clock)
            assertEquals(OperationState.SUCCEEDED, repository.load(attempt.id)?.state)
            assertEquals(attempt.id, repository.operationForDesired(desired)?.id)
            assertEquals(StepStatus.SUCCEEDED.name, repository.steps(attempt.id).single().status)
        }

        assertEquals(
            RoomOperationRepository.MAX_RETAINED_SYSTEM_RECONCILIATIONS_PER_GENERATION + 1L,
            database.dao().operationCount(),
        )
        assertEquals(
            RoomOperationRepository.MAX_RETAINED_SYSTEM_RECONCILIATIONS_PER_GENERATION.toLong(),
            database.dao().stepCount(),
        )
        assertEquals(OperationState.SUCCEEDED, repository.load(userOperation.id)?.state)
        assertEquals(desired, repository.loadDesiredRuntime(RuntimeId.DEFAULT))
    }

    @Test
    fun currentObservationIsDurableDerivedState() = runBlocking {
        repository.recordObservation(RuntimeObservation.Running(RuntimeId.DEFAULT, 42, guestReady = true))
        assertEquals(
            RuntimeObservation.Running(RuntimeId.DEFAULT, 42, guestReady = true),
            RoomOperationRepository(database, clock).current(RuntimeId.DEFAULT),
        )
        assertNull(repository.loadDesiredRuntime(RuntimeId.DEFAULT))
    }

    private fun spec(generation: Long) = RuntimeSpec(
        generation = generation,
        desiredState = DesiredRuntimeState.RUNNING,
        profileId = VmProfileId("alpine-direct"),
        memoryMiB = 512,
        vcpus = 1,
        dataDiskGiB = 4,
    )

    private fun operation(id: String, key: String, generation: Long, digest: String = "a".repeat(64)) = OperationRecord(
        OperationId(id), key, digest, RuntimeId.DEFAULT, generation, OperationState.ACCEPTED, null,
    )
}

private class MutableClock(var now: Long) : org.nodehost.core.Clock {
    override fun epochMillis(): Long = now
}
