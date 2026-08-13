package org.nodehost.model

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

class OperationTransitionsTest {
    private val allowed = setOf(
        ACCEPTED to PREFLIGHT, ACCEPTED to FETCHING, ACCEPTED to VERIFYING,
        ACCEPTED to PREPARING_DISKS, ACCEPTED to PREPARING_BOOT,
        ACCEPTED to STARTING_QEMU, ACCEPTED to WAITING_FOR_QMP,
        ACCEPTED to BOOTSTRAPPING_GUEST, ACCEPTED to WAITING_FOR_SSH,
        ACCEPTED to WAITING_FOR_GUEST_MESH, ACCEPTED to SUCCEEDED,
        ACCEPTED to CANCELLING,
        PREFLIGHT to FETCHING, PREFLIGHT to PREPARING_DISKS, PREFLIGHT to SUCCEEDED,
        PREFLIGHT to WAITING_FOR_USER, PREFLIGHT to FAILED_RETRYABLE,
        PREFLIGHT to FAILED_PERMANENT, PREFLIGHT to CANCELLING,
        FETCHING to VERIFYING, FETCHING to PREPARING_DISKS, FETCHING to WAITING_FOR_USER,
        FETCHING to FAILED_RETRYABLE,
        FETCHING to FAILED_PERMANENT, FETCHING to CANCELLING,
        VERIFYING to PREPARING_DISKS, VERIFYING to FAILED_RETRYABLE,
        VERIFYING to FAILED_PERMANENT, VERIFYING to CANCELLING,
        PREPARING_DISKS to PREPARING_BOOT, PREPARING_DISKS to FAILED_RETRYABLE,
        PREPARING_DISKS to FAILED_PERMANENT, PREPARING_DISKS to CANCELLING,
        PREPARING_BOOT to STARTING_QEMU, PREPARING_BOOT to SUCCEEDED,
        PREPARING_BOOT to FAILED_RETRYABLE, PREPARING_BOOT to FAILED_PERMANENT, PREPARING_BOOT to CANCELLING,
        STARTING_QEMU to WAITING_FOR_QMP, STARTING_QEMU to FAILED_RETRYABLE,
        STARTING_QEMU to FAILED_PERMANENT, STARTING_QEMU to CANCELLING,
        WAITING_FOR_QMP to BOOTSTRAPPING_GUEST, WAITING_FOR_QMP to FAILED_RETRYABLE,
        WAITING_FOR_QMP to FAILED_PERMANENT, WAITING_FOR_QMP to CANCELLING,
        BOOTSTRAPPING_GUEST to WAITING_FOR_SSH, BOOTSTRAPPING_GUEST to SUCCEEDED,
        BOOTSTRAPPING_GUEST to FAILED_RETRYABLE, BOOTSTRAPPING_GUEST to FAILED_PERMANENT,
        BOOTSTRAPPING_GUEST to CANCELLING,
        WAITING_FOR_SSH to WAITING_FOR_GUEST_MESH, WAITING_FOR_SSH to SUCCEEDED,
        WAITING_FOR_SSH to FAILED_RETRYABLE, WAITING_FOR_SSH to FAILED_PERMANENT, WAITING_FOR_SSH to CANCELLING,
        WAITING_FOR_GUEST_MESH to SUCCEEDED, WAITING_FOR_GUEST_MESH to WAITING_FOR_USER,
        WAITING_FOR_GUEST_MESH to FAILED_RETRYABLE, WAITING_FOR_GUEST_MESH to FAILED_PERMANENT,
        WAITING_FOR_GUEST_MESH to CANCELLING,
        WAITING_FOR_USER to PREFLIGHT, WAITING_FOR_USER to CANCELLING,
        FAILED_RETRYABLE to PREFLIGHT, FAILED_RETRYABLE to FETCHING,
        FAILED_RETRYABLE to VERIFYING, FAILED_RETRYABLE to PREPARING_DISKS,
        FAILED_RETRYABLE to PREPARING_BOOT, FAILED_RETRYABLE to STARTING_QEMU,
        FAILED_RETRYABLE to WAITING_FOR_QMP, FAILED_RETRYABLE to BOOTSTRAPPING_GUEST,
        FAILED_RETRYABLE to WAITING_FOR_SSH, FAILED_RETRYABLE to WAITING_FOR_GUEST_MESH,
        FAILED_RETRYABLE to ROLLING_BACK, FAILED_RETRYABLE to CANCELLING,
        CANCELLING to CANCELLED, CANCELLING to ROLLING_BACK,
        ROLLING_BACK to ROLLED_BACK, ROLLING_BACK to FAILED_PERMANENT,
    )

    @Test
    fun transitionTableAllowsExactlyDocumentedEdges() {
        OperationState.entries.forEach { from ->
            OperationState.entries.forEach { to ->
                assertEquals("$from -> $to", (from to to) in allowed, OperationTransitions.isAllowed(from, to))
            }
        }
    }

    @Test
    fun terminalStatesHaveNoOutgoingTransitions() {
        OperationState.entries.filter { it.terminal }.forEach { terminal ->
            assertFalse(OperationState.entries.any { OperationTransitions.isAllowed(terminal, it) })
        }
    }

    @Test
    fun recordTransitionRetainsStableIdentityAndRecordsFailure() {
        val record = acceptedRecord()
        val failed = record.transitionTo(PREFLIGHT, "runtime.preflight")
            .transitionTo(FAILED_RETRYABLE, errorCode = "NETWORK_TIMEOUT")

        assertEquals(record.id, failed.id)
        assertEquals("runtime.preflight", failed.currentStepId)
        assertEquals("NETWORK_TIMEOUT", failed.errorCode)
    }

    @Test
    fun failedStateRequiresErrorCode() {
        assertThrows(IllegalArgumentException::class.java) {
            acceptedRecord().transitionTo(PREFLIGHT).transitionTo(FAILED_RETRYABLE)
        }
    }

    @Test
    fun terminalFlagsAreStable() {
        assertTrue(listOf(SUCCEEDED, FAILED_PERMANENT, CANCELLED, ROLLED_BACK).all { it.terminal })
        assertFalse(OperationState.entries.filterNot { it in listOf(SUCCEEDED, FAILED_PERMANENT, CANCELLED, ROLLED_BACK) }.any { it.terminal })
    }

    private fun acceptedRecord() = OperationRecord(
        id = OperationId("op-001"),
        idempotencyKey = "idempotency-key-001",
        requestDigest = "a".repeat(64),
        runtimeId = RuntimeId.DEFAULT,
        desiredGeneration = 1,
        state = ACCEPTED,
        currentStepId = null,
    )

    private companion object {
        val ACCEPTED = OperationState.ACCEPTED
        val PREFLIGHT = OperationState.PREFLIGHT
        val FETCHING = OperationState.FETCHING
        val VERIFYING = OperationState.VERIFYING
        val PREPARING_DISKS = OperationState.PREPARING_DISKS
        val PREPARING_BOOT = OperationState.PREPARING_BOOT
        val STARTING_QEMU = OperationState.STARTING_QEMU
        val WAITING_FOR_QMP = OperationState.WAITING_FOR_QMP
        val BOOTSTRAPPING_GUEST = OperationState.BOOTSTRAPPING_GUEST
        val WAITING_FOR_SSH = OperationState.WAITING_FOR_SSH
        val WAITING_FOR_GUEST_MESH = OperationState.WAITING_FOR_GUEST_MESH
        val SUCCEEDED = OperationState.SUCCEEDED
        val WAITING_FOR_USER = OperationState.WAITING_FOR_USER
        val FAILED_RETRYABLE = OperationState.FAILED_RETRYABLE
        val FAILED_PERMANENT = OperationState.FAILED_PERMANENT
        val CANCELLING = OperationState.CANCELLING
        val CANCELLED = OperationState.CANCELLED
        val ROLLING_BACK = OperationState.ROLLING_BACK
        val ROLLED_BACK = OperationState.ROLLED_BACK
    }
}
