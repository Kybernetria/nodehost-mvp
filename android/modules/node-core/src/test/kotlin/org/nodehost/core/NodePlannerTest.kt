package org.nodehost.core

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.nodehost.model.DesiredRuntimeState
import org.nodehost.model.RuntimeId
import org.nodehost.model.RuntimeObservation
import org.nodehost.model.RuntimeSpec
import org.nodehost.model.VmProfileId

class NodePlannerTest {
    private val runningDesired = RuntimeSpec(
        id = RuntimeId.DEFAULT,
        generation = 1,
        desiredState = DesiredRuntimeState.RUNNING,
        profileId = VmProfileId("ubuntu-2404-arm64-uefi"),
        memoryMiB = 1024,
        vcpus = 2,
        dataDiskGiB = 8,
    )

    @Test
    fun absentRuntimeGetsStartPlan() {
        val plan = NodePlanner.plan(
            runningDesired,
            RuntimeObservation.Absent(RuntimeId.DEFAULT),
        )
        assertEquals(RuntimeStep.VerifyProfile, plan.steps.first())
        assertEquals(RuntimeStep.WaitForGuest, plan.steps.last())
    }

    @Test
    fun runningGenerationAndProfileUpdateRequestsGracefulShutdown() {
        val updatedDesired = runningDesired.copy(
            generation = 2,
            profileId = VmProfileId("alpine-direct-qualification"),
            memoryMiB = 2048,
        )

        val plan = NodePlanner.plan(
            updatedDesired,
            RuntimeObservation.Running(
                RuntimeId.DEFAULT,
                processId = 123,
                guestReady = true,
                appliedGeneration = 1,
            ),
        )

        assertEquals(listOf(RuntimeStep.RequestShutdown), plan.steps)
        assertTrue(plan.steps.none { it == RuntimeStep.StartProcess })
    }

    @Test
    fun runningAtDesiredGenerationButBeforeGuestHealthWaitsForGuest() {
        val plan = NodePlanner.plan(
            runningDesired,
            RuntimeObservation.Running(
                RuntimeId.DEFAULT,
                processId = 123,
                guestReady = false,
                appliedGeneration = runningDesired.generation,
            ),
        )

        assertEquals(listOf(RuntimeStep.WaitForGuest), plan.steps)
    }

    @Test
    fun runningAtDesiredGenerationNeedsNoWork() {
        val plan = NodePlanner.plan(
            runningDesired,
            RuntimeObservation.Running(
                RuntimeId.DEFAULT,
                processId = 123,
                guestReady = true,
                appliedGeneration = runningDesired.generation,
            ),
        )

        assertTrue(plan.steps.isEmpty())
    }

    @Test
    fun runningWithUnknownLegacyGenerationRemainsConverged() {
        val plan = NodePlanner.plan(
            runningDesired,
            RuntimeObservation.Running(RuntimeId.DEFAULT, processId = 123, guestReady = true),
        )

        assertTrue(plan.steps.isEmpty())
    }

    @Test
    fun generationUpdateWaitsForGracefulStopBeforeStartingReplacement() {
        val updatedDesired = runningDesired.copy(generation = 2)
        val runningPlan = NodePlanner.plan(
            updatedDesired,
            RuntimeObservation.Running(
                RuntimeId.DEFAULT,
                processId = 123,
                guestReady = true,
                appliedGeneration = 1,
            ),
        )
        val stoppingPlan = NodePlanner.plan(
            updatedDesired,
            RuntimeObservation.Stopping(
                RuntimeId.DEFAULT,
                processId = 123,
                gracefulDeadlineExceeded = false,
            ),
        )
        val deadlinePlan = NodePlanner.plan(
            updatedDesired,
            RuntimeObservation.Stopping(
                RuntimeId.DEFAULT,
                processId = 123,
                gracefulDeadlineExceeded = true,
            ),
        )
        val stoppedPlan = NodePlanner.plan(
            updatedDesired,
            RuntimeObservation.Stopped(RuntimeId.DEFAULT, runningDesired.profileId),
        )

        assertEquals(listOf(RuntimeStep.RequestShutdown), runningPlan.steps)
        assertTrue(stoppingPlan.steps.isEmpty())
        assertEquals(listOf(RuntimeStep.ForceStop), deadlinePlan.steps)
        assertEquals(RuntimeStep.VerifyProfile, stoppedPlan.steps.first())
        assertEquals(RuntimeStep.StartProcess, stoppedPlan.steps[4])
        assertTrue(
            (runningPlan.steps + stoppingPlan.steps + deadlinePlan.steps)
                .none { it == RuntimeStep.StartProcess },
        )
    }

    @Test
    fun stopRequestsGracefulShutdownWithoutImmediateForce() {
        val desired = runningDesired.copy(desiredState = DesiredRuntimeState.STOPPED)
        val plan = NodePlanner.plan(
            desired,
            RuntimeObservation.Running(RuntimeId.DEFAULT, processId = 123, guestReady = true),
        )
        assertEquals(listOf(RuntimeStep.RequestShutdown), plan.steps)
    }

    @Test
    fun forceStopAppearsOnlyAfterGracefulDeadline() {
        val desired = runningDesired.copy(desiredState = DesiredRuntimeState.STOPPED)
        val beforeDeadline = NodePlanner.plan(
            desired,
            RuntimeObservation.Stopping(
                RuntimeId.DEFAULT,
                processId = 123,
                gracefulDeadlineExceeded = false,
            ),
        )
        val afterDeadline = NodePlanner.plan(
            desired,
            RuntimeObservation.Stopping(
                RuntimeId.DEFAULT,
                processId = 123,
                gracefulDeadlineExceeded = true,
            ),
        )
        assertTrue(beforeDeadline.steps.isEmpty())
        assertEquals(listOf(RuntimeStep.ForceStop), afterDeadline.steps)
    }
}
