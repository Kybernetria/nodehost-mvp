package org.nodehost.model

@JvmInline
value class OperationId(val value: String) {
    init { require(Regex("[A-Za-z0-9][A-Za-z0-9._-]{2,127}").matches(value)) { "invalid operation id" } }
}

enum class OperationState {
    ACCEPTED, PREFLIGHT, FETCHING, VERIFYING, PREPARING_DISKS, PREPARING_BOOT,
    STARTING_QEMU, WAITING_FOR_QMP, BOOTSTRAPPING_GUEST, WAITING_FOR_SSH,
    WAITING_FOR_GUEST_MESH, SUCCEEDED, WAITING_FOR_USER, FAILED_RETRYABLE,
    FAILED_PERMANENT, CANCELLING, CANCELLED, ROLLING_BACK, ROLLED_BACK;

    val terminal: Boolean get() = this in TERMINAL_STATES

    private companion object {
        val TERMINAL_STATES = setOf(SUCCEEDED, FAILED_PERMANENT, CANCELLED, ROLLED_BACK)
    }
}

data class OperationRecord(
    val id: OperationId,
    val idempotencyKey: String,
    val requestDigest: String,
    val runtimeId: RuntimeId?,
    val desiredGeneration: Long?,
    val state: OperationState,
    val currentStepId: String?,
    val errorCode: String? = null,
) {
    init {
        require(idempotencyKey.length in 16..200)
        require(Regex("[a-f0-9]{64}").matches(requestDigest))
        require(desiredGeneration == null || desiredGeneration >= 1)
        require(currentStepId == null || Regex("[a-z][a-z0-9_.-]{0,127}").matches(currentStepId))
        require(errorCode == null || Regex("[A-Z][A-Z0-9_]{0,63}").matches(errorCode))
        require(state !in setOf(OperationState.FAILED_RETRYABLE, OperationState.FAILED_PERMANENT) || errorCode != null) {
            "failed operation requires an error code"
        }
    }

    fun transitionTo(next: OperationState, stepId: String? = currentStepId, errorCode: String? = null): OperationRecord {
        OperationTransitions.requireAllowed(state, next)
        return copy(state = next, currentStepId = stepId, errorCode = errorCode)
    }
}

object OperationTransitions {
    private val allowed: Map<OperationState, Set<OperationState>> = mapOf(
        OperationState.ACCEPTED to setOf(
            OperationState.PREFLIGHT, OperationState.FETCHING, OperationState.VERIFYING,
            OperationState.PREPARING_DISKS, OperationState.PREPARING_BOOT,
            OperationState.STARTING_QEMU, OperationState.WAITING_FOR_QMP,
            OperationState.BOOTSTRAPPING_GUEST, OperationState.WAITING_FOR_SSH,
            OperationState.WAITING_FOR_GUEST_MESH, OperationState.SUCCEEDED,
            OperationState.CANCELLING,
        ),
        OperationState.PREFLIGHT to setOf(
            OperationState.FETCHING, OperationState.PREPARING_DISKS,
            OperationState.SUCCEEDED, OperationState.WAITING_FOR_USER,
            OperationState.FAILED_RETRYABLE, OperationState.FAILED_PERMANENT,
            OperationState.CANCELLING,
        ),
        OperationState.FETCHING to setOf(
            OperationState.VERIFYING, OperationState.PREPARING_DISKS,
            OperationState.WAITING_FOR_USER, OperationState.FAILED_RETRYABLE,
            OperationState.FAILED_PERMANENT, OperationState.CANCELLING,
        ),
        OperationState.VERIFYING to setOf(
            OperationState.PREPARING_DISKS, OperationState.FAILED_RETRYABLE,
            OperationState.FAILED_PERMANENT, OperationState.CANCELLING,
        ),
        OperationState.PREPARING_DISKS to setOf(
            OperationState.PREPARING_BOOT, OperationState.FAILED_RETRYABLE,
            OperationState.FAILED_PERMANENT, OperationState.CANCELLING,
        ),
        OperationState.PREPARING_BOOT to setOf(
            OperationState.STARTING_QEMU, OperationState.SUCCEEDED,
            OperationState.FAILED_RETRYABLE, OperationState.FAILED_PERMANENT, OperationState.CANCELLING,
        ),
        OperationState.STARTING_QEMU to setOf(
            OperationState.WAITING_FOR_QMP, OperationState.FAILED_RETRYABLE,
            OperationState.FAILED_PERMANENT, OperationState.CANCELLING,
        ),
        OperationState.WAITING_FOR_QMP to setOf(
            OperationState.BOOTSTRAPPING_GUEST, OperationState.FAILED_RETRYABLE,
            OperationState.FAILED_PERMANENT, OperationState.CANCELLING,
        ),
        OperationState.BOOTSTRAPPING_GUEST to setOf(
            OperationState.WAITING_FOR_SSH, OperationState.SUCCEEDED,
            OperationState.FAILED_RETRYABLE, OperationState.FAILED_PERMANENT,
            OperationState.CANCELLING,
        ),
        OperationState.WAITING_FOR_SSH to setOf(
            OperationState.WAITING_FOR_GUEST_MESH, OperationState.SUCCEEDED,
            OperationState.FAILED_RETRYABLE, OperationState.FAILED_PERMANENT, OperationState.CANCELLING,
        ),
        OperationState.WAITING_FOR_GUEST_MESH to setOf(
            OperationState.SUCCEEDED, OperationState.WAITING_FOR_USER,
            OperationState.FAILED_RETRYABLE, OperationState.FAILED_PERMANENT, OperationState.CANCELLING,
        ),
        OperationState.WAITING_FOR_USER to setOf(OperationState.PREFLIGHT, OperationState.CANCELLING),
        OperationState.FAILED_RETRYABLE to setOf(
            OperationState.PREFLIGHT, OperationState.FETCHING, OperationState.VERIFYING,
            OperationState.PREPARING_DISKS, OperationState.PREPARING_BOOT,
            OperationState.STARTING_QEMU, OperationState.WAITING_FOR_QMP,
            OperationState.BOOTSTRAPPING_GUEST, OperationState.WAITING_FOR_SSH,
            OperationState.WAITING_FOR_GUEST_MESH, OperationState.ROLLING_BACK,
            OperationState.CANCELLING,
        ),
        OperationState.CANCELLING to setOf(OperationState.CANCELLED, OperationState.ROLLING_BACK),
        OperationState.ROLLING_BACK to setOf(OperationState.ROLLED_BACK, OperationState.FAILED_PERMANENT),
    )

    fun isAllowed(from: OperationState, to: OperationState): Boolean = to in allowed.orEmpty(from)

    fun requireAllowed(from: OperationState, to: OperationState) {
        require(isAllowed(from, to)) { "invalid operation transition: $from -> $to" }
    }

    private fun <K, V> Map<K, Set<V>>.orEmpty(key: K): Set<V> = this[key] ?: emptySet()
}
