package org.nodehost.core

import org.nodehost.model.DesiredRuntimeState
import org.nodehost.model.RuntimeObservation
import org.nodehost.model.RuntimeSpec

object NodePlanner {
    fun plan(desired: RuntimeSpec, observed: RuntimeObservation): RuntimePlan =
        when (desired.desiredState) {
            DesiredRuntimeState.RUNNING -> planRunning(desired, observed)
            DesiredRuntimeState.STOPPED -> planStopped(observed)
            DesiredRuntimeState.ABSENT -> planAbsent(observed)
        }

    private fun planRunning(desired: RuntimeSpec, observed: RuntimeObservation): RuntimePlan =
        when (observed) {
            is RuntimeObservation.Running ->
                when {
                    observed.appliedGeneration != null && observed.appliedGeneration != desired.generation ->
                        RuntimePlan(listOf(RuntimeStep.RequestShutdown))
                    !observed.guestReady -> RuntimePlan(listOf(RuntimeStep.WaitForGuest))
                    else -> RuntimePlan(emptyList())
                }
            is RuntimeObservation.Starting -> RuntimePlan(
                listOf(RuntimeStep.WaitForQmp, RuntimeStep.WaitForGuest),
            )
            is RuntimeObservation.Stopping ->
                if (observed.gracefulDeadlineExceeded) {
                    RuntimePlan(listOf(RuntimeStep.ForceStop))
                } else {
                    RuntimePlan(emptyList())
                }
            else -> RuntimePlan(
                listOf(
                    RuntimeStep.VerifyProfile,
                    RuntimeStep.ResolveArtifacts,
                    RuntimeStep.PrepareDisks,
                    RuntimeStep.PrepareBoot,
                    RuntimeStep.StartProcess,
                    RuntimeStep.WaitForQmp,
                    RuntimeStep.WaitForGuest,
                ),
            )
        }

    private fun planStopped(observed: RuntimeObservation): RuntimePlan =
        when (observed) {
            is RuntimeObservation.Running,
            is RuntimeObservation.Starting,
            -> RuntimePlan(listOf(RuntimeStep.RequestShutdown))

            is RuntimeObservation.Stopping ->
                if (observed.gracefulDeadlineExceeded) {
                    RuntimePlan(listOf(RuntimeStep.ForceStop))
                } else {
                    RuntimePlan(emptyList())
                }

            else -> RuntimePlan(emptyList())
        }

    private fun planAbsent(observed: RuntimeObservation): RuntimePlan =
        when (observed) {
            is RuntimeObservation.Absent -> RuntimePlan(emptyList())
            is RuntimeObservation.Running,
            is RuntimeObservation.Starting,
            -> RuntimePlan(listOf(RuntimeStep.RequestShutdown))

            is RuntimeObservation.Stopping ->
                if (observed.gracefulDeadlineExceeded) {
                    RuntimePlan(listOf(RuntimeStep.ForceStop))
                } else {
                    RuntimePlan(emptyList())
                }

            else -> RuntimePlan(listOf(RuntimeStep.RemoveSystem))
        }
}
