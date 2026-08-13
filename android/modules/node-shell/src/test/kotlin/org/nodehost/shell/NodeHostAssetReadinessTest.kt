package org.nodehost.shell

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class NodeHostAssetReadinessTest {
    @Test
    fun failedExtractionIsDiscardedAndSameProcessRetrySucceeds() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            var attempts = 0
            val readiness = NodeHostAssetReadinessCoordinator(scope, extractionTimeoutMillis = 500) {
                attempts++
                check(attempts > 1) { "first extraction failed" }
            }

            assertTrue(runCatching { readiness.awaitNodeHostAssets() }.isFailure)
            readiness.awaitNodeHostAssets()
            readiness.awaitNodeHostAssets()

            assertEquals(2, attempts)
        } finally {
            scope.cancel()
        }
    }

    @Test
    fun stalledExtractionTimesOutAndIsInterruptibleBeforeRetry() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val cancelled = CompletableDeferred<Unit>()
            var attempts = 0
            val readiness = NodeHostAssetReadinessCoordinator(scope, extractionTimeoutMillis = 25) {
                attempts++
                if (attempts == 1) {
                    try {
                        awaitCancellation()
                    } finally {
                        cancelled.complete(Unit)
                    }
                }
            }

            assertTrue(runCatching { readiness.awaitNodeHostAssets() }.isFailure)
            withTimeout(1_000) { cancelled.await() }
            readiness.awaitNodeHostAssets()

            assertEquals(2, attempts)
        } finally {
            scope.cancel()
        }
    }

    @Test
    fun cancelledServiceWaiterDoesNotPoisonSharedExtraction() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val entered = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            var attempts = 0
            val readiness = NodeHostAssetReadinessCoordinator(scope, extractionTimeoutMillis = 500) {
                attempts++
                entered.complete(Unit)
                release.await()
            }

            val waiter = launch { readiness.awaitNodeHostAssets() }
            entered.await()
            waiter.cancel()
            waiter.join()
            release.complete(Unit)
            readiness.awaitNodeHostAssets()

            assertEquals(1, attempts)
        } finally {
            scope.cancel()
        }
    }
}
