package org.nodehost.shell

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withTimeout

/** Optional composition hook for the imported application asset extractor. */
fun interface NodeHostAssetReadiness {
    suspend fun awaitNodeHostAssets()
}

/**
 * Shares one bounded extraction attempt between service recreations and callers, while allowing
 * a failed or timed-out attempt to be replaced. A successful attempt is retained because the
 * extracted files are process-wide resources; a failed attempt is never retained as readiness.
 */
class NodeHostAssetReadinessCoordinator(
    private val scope: CoroutineScope,
    private val extractionTimeoutMillis: Long = DEFAULT_EXTRACTION_TIMEOUT_MILLIS,
    private val extract: suspend () -> Unit,
) : NodeHostAssetReadiness {
    init {
        require(extractionTimeoutMillis > 0)
    }

    private val lock = Any()
    private var currentAttempt: Deferred<Unit>? = null

    override suspend fun awaitNodeHostAssets() {
        val attempt = synchronized(lock) {
            currentAttempt?.takeUnless { it.isCancelled } ?: scope.async(Dispatchers.IO) {
                withTimeout(extractionTimeoutMillis) { extract() }
            }.also { currentAttempt = it }
        }
        try {
            attempt.await()
        } catch (failure: CancellationException) {
            // A caller stopping its service waiter must not cancel a shared extraction. The
            // attempt's own timeout/failure, however, must be discarded so the next waiter can
            // retry rather than inheriting a permanently completed exceptional Deferred.
            if (currentCoroutineContext().isActive) discardFailedAttempt(attempt)
            throw failure
        } catch (failure: Throwable) {
            discardFailedAttempt(attempt)
            throw failure
        }
    }

    private fun discardFailedAttempt(attempt: Deferred<Unit>) {
        synchronized(lock) {
            if (currentAttempt === attempt) currentAttempt = null
        }
        attempt.cancel()
    }

    private companion object {
        const val DEFAULT_EXTRACTION_TIMEOUT_MILLIS = 2 * 60_000L
    }
}
