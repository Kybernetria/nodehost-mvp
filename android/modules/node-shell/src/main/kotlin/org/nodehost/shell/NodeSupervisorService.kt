package org.nodehost.shell

import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import android.os.IBinder
import androidx.core.app.NotificationCompat
import androidx.core.app.ServiceCompat
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

class NodeSupervisorService : Service() {
    private lateinit var supervisorScope: CoroutineScope
    private lateinit var reconciler: ReconciliationActor
    private lateinit var bootstrapServer: BootstrapMetadataServer
    private var assetReadinessJob: Job? = null

    override fun onCreate() {
        super.onCreate()
        NodeHostGraph.initialize(this)
        createChannel()
        supervisorScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        reconciler = NodeHostGraph.createSupervisor(supervisorScope)
        bootstrapServer = NodeHostGraph.createBootstrapServer().also(BootstrapMetadataServer::start)
        NodeHostGraph.restoreAuthorityAndApi()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val notification = NotificationCompat.Builder(this, CHANNEL)
            .setSmallIcon(android.R.drawable.stat_notify_sync)
            .setContentTitle("NodeHost")
            .setContentText("Reconciling desired node state")
            .setOngoing(true)
            .build()
        val type = if (Build.VERSION.SDK_INT >= 34) ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE else 0
        ServiceCompat.startForeground(this, ID, notification, type)
        // Podroid extracts the bundled Alpine artifacts asynchronously. Do not let the
        // durable reconciler race that publication; imported applications without the
        // optional hook retain the existing immediate-start behaviour.
        val readiness = application as? NodeHostAssetReadiness
        assetReadinessJob?.cancel()
        assetReadinessJob = supervisorScope.launch {
            repeat(ASSET_READINESS_ATTEMPTS) { attempt ->
                try {
                    readiness?.awaitNodeHostAssets()
                    reconciler.wake(WakeReason.SERVICE_STARTED)
                    return@launch
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (failure: Exception) {
                    android.util.Log.e(TAG, "Node-host asset readiness attempt ${attempt + 1} failed", failure)
                    if (attempt + 1 < ASSET_READINESS_ATTEMPTS) delay(ASSET_RETRY_MILLIS)
                }
            }
            android.util.Log.e(TAG, "Node-host asset readiness exhausted; service start remains gated")
        }
        return START_STICKY
    }

    override fun onTaskRemoved(rootIntent: Intent?) {
        // A UI lifecycle event is deliberately not forwarded as a desired-state command.
    }

    override fun onDestroy() {
        // The pending intent is durable. Cancellation leaves it recoverable by the sticky restart.
        assetReadinessJob?.cancel()
        reconciler.close()
        bootstrapServer.close()
        NodeHostGraph.stopServiceOwnedComponents()
        supervisorScope.cancel()
        super.onDestroy()
    }

    override fun onBind(intent: Intent?): IBinder? = null

    private fun createChannel() {
        if (Build.VERSION.SDK_INT >= 26) {
            getSystemService(NotificationManager::class.java).createNotificationChannel(
                NotificationChannel(CHANNEL, "Node hosting", NotificationManager.IMPORTANCE_LOW),
            )
        }
    }

    companion object {
        const val CHANNEL = "nodehost-runtime"
        const val ID = 47001
        private const val TAG = "NodeHostSupervisorService"
        private const val ASSET_READINESS_ATTEMPTS = 3
        private const val ASSET_RETRY_MILLIS = 1_000L
    }
}
