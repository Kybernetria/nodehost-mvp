package org.nodehost.shell

import android.content.Context
import android.os.SystemClock
import androidx.room.withTransaction
import java.io.File
import java.io.FileOutputStream
import java.net.HttpURLConnection
import java.net.InetAddress
import java.net.URI
import java.net.URL
import java.security.MessageDigest
import javax.net.ssl.HttpsURLConnection
import javax.net.ssl.SNIHostName
import javax.net.ssl.SSLSocket
import javax.net.ssl.SSLSocketFactory
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.json.JSONObject
import org.nodehost.api.HostCapability
import org.nodehost.api.HostDiagnostics
import org.nodehost.api.HostImage
import org.nodehost.api.HostMutationUseCases
import org.nodehost.api.HostProfile
import org.nodehost.api.HostResourceQueries
import org.nodehost.api.HostStatus
import org.nodehost.api.HostVm
import org.nodehost.api.ImageImportRequest
import org.nodehost.core.ApplyRuntimeUseCase
import org.nodehost.core.HostMesh
import org.nodehost.model.DesiredRuntimeState
import org.nodehost.model.OperationId
import org.nodehost.model.OperationRecord
import org.nodehost.model.OperationState
import org.nodehost.model.RuntimeId
import org.nodehost.store.NodeHostDatabase
import org.nodehost.store.OperationEntity
import org.nodehost.store.RoomOperationRepository

private val OperationRecord.blocksArtifactEffects: Boolean
    get() = state.terminal || state == OperationState.CANCELLING

@JvmInline
value class TailnetBindAddress(val value: String) {
    init {
        org.nodehost.api.HostControlServer.requireSafeBindAddress(value)
        require(!InetAddress.getByName(value).isLoopbackAddress) { "API requires an actual tailnet address" }
    }
}

class AndroidHostResourceQueries(
    context: Context,
    private val database: NodeHostDatabase,
    private val operationsRepository: RoomOperationRepository,
    private val mesh: HostMesh,
    private val clockMillis: () -> Long,
) : HostResourceQueries {
    private val artifacts = File(context.filesDir, "nodehost-artifacts")
    private val deviceId = android.provider.Settings.Secure.getString(
        context.contentResolver, android.provider.Settings.Secure.ANDROID_ID,
    )?.take(128).orEmpty().ifEmpty { "android-node" }

    override suspend fun status(): HostStatus {
        val meshState = mesh.status().state.name.lowercase()
        val runtime = operationsRepository.current(RuntimeId.DEFAULT)?.javaClass?.simpleName?.lowercase() ?: "unknown"
        return HostStatus(deviceId, meshState, runtime)
    }
    override suspend fun capabilities() = listOf(
        HostCapability("vm.single", true), HostCapability("image.https-import", true),
        HostCapability("recovery.ssh", true),
    )
    override suspend fun profiles() = listOf(
        HostProfile("alpine-direct-qualification", 1, "DIRECT_KERNEL"),
        HostProfile("ubuntu-2404-arm64-uefi", 1, "UEFI"),
        HostProfile("k3s-worker-lab", 1, "UEFI"),
    )
    override suspend fun images(): List<HostImage> = withContext(Dispatchers.IO) {
        val manifests = artifacts.listFiles()?.filter { it.isFile && it.name.endsWith(".manifest.json") }
            ?.sortedBy { it.name }?.take(128).orEmpty()
        manifests.mapNotNull { manifest ->
            runCatching {
                require(manifest.length() in 1..MAX_MANIFEST_BYTES)
                val value = JSONObject(manifest.readText())
                require(value.keys().asSequence().toSet() == setOf("version", "sha256", "sizeBytes", "relativePath"))
                require(value.getInt("version") == 1)
                val id = manifest.name.removeSuffix(".manifest.json")
                val digest = value.getString("sha256")
                val sizeBytes = value.getLong("sizeBytes")
                require(IMAGE_ID.matches(id) && SHA256.matches(digest) && sizeBytes in 1..MAX_IMAGE_BYTES)
                require(value.getString("relativePath") == "versions/$id/$digest/payload")
                HostImage(id, digest, sizeBytes)
            }.getOrNull()
        }
    }
    override suspend fun vms(): List<HostVm> = vm(RuntimeId.DEFAULT)?.let(::listOf).orEmpty()
    override suspend fun vm(id: RuntimeId): HostVm? {
        require(id == RuntimeId.DEFAULT) { "MVP supports one runtime" }
        return operationsRepository.loadDesiredRuntime(id)?.let {
            HostVm(it.id.value, it.generation, it.desiredState.name.lowercase(), it.profileId.value, it.memoryMiB, it.vcpus, it.dataDiskGiB)
        }
    }
    override suspend fun operations(): List<OperationRecord> = withContext(Dispatchers.IO) {
        database.openHelper.readableDatabase.query(
            "SELECT id,idempotencyKey,requestDigest,runtimeId,desiredGeneration,state,currentStepId,errorCode FROM operations ORDER BY updatedAtEpochMillis DESC LIMIT 256",
        ).use { cursor -> buildList { while (cursor.moveToNext()) add(cursor.operation()) } }
    }
    override suspend fun operation(id: String): OperationRecord? = operationsRepository.load(OperationId(id))
    override suspend fun diagnostics() = HostDiagnostics(clockMillis(), mapOf(
        "api.transport" to "tls-tailnet", "runtime.limit" to "one", "artifact.count" to images().size.toString(),
    ))

    private fun android.database.Cursor.operation() = OperationRecord(
        OperationId(getString(0)), getString(1), getString(2), getString(3)?.let(::RuntimeId),
        if (isNull(4)) null else getLong(4), OperationState.valueOf(getString(5)), getString(6), getString(7),
    )
    private companion object {
        val IMAGE_ID = Regex("[a-z0-9][a-z0-9.-]{0,127}")
        val SHA256 = Regex("[a-f0-9]{64}")
        const val MAX_MANIFEST_BYTES = 4096L
        const val MAX_IMAGE_BYTES = 64L * 1024 * 1024 * 1024
    }
}

internal class AndroidHostMutations(
    context: Context,
    private val database: NodeHostDatabase,
    private val operations: RoomOperationRepository,
    private val applyRuntime: ApplyRuntimeUseCase,
    private val enrolledRepositoryOrigin: suspend () -> URI,
    private val operationIds: SecureOperationIdFactory = SecureOperationIdFactory(),
    private val clockMillis: () -> Long = System::currentTimeMillis,
    private val serviceScope: CoroutineScope? = null,
    private val addressResolver: (String) -> List<InetAddress> = { InetAddress.getAllByName(it).toList() },
    private val beforePublicationClaim: suspend (OperationId) -> Unit = {},
) : HostMutationUseCases {
    private val artifacts = File(context.filesDir, "nodehost-artifacts")
    private val pendingImports = File(context.filesDir, "nodehost-imports")
    private val lock = Mutex()
    private val importEffectLock = Mutex()

    override suspend fun importImage(request: ImageImportRequest, idempotencyKey: String, canonicalRequest: ByteArray): OperationRecord {
        val digest = sha256(canonicalRequest)
        val (operation, newlyAccepted) = lock.withLock {
            existing(idempotencyKey)?.let {
                require(it.requestDigest == digest) { "idempotency key reused with different request" }
                return@withLock it to false
            }
            val accepted = OperationRecord(operationIds.newId(), idempotencyKey, digest, null, null, OperationState.ACCEPTED, null)
            // Persist resumable effect input before the journal can advertise work as accepted.
            persistPending(accepted.id, request)
            try {
                insert(accepted)
            } catch (failure: Throwable) {
                pendingFile(accepted.id).delete()
                throw failure
            }
            accepted to true
        }
        if (!newlyAccepted) {
            if (operation.state == OperationState.FAILED_RETRYABLE && pendingFile(operation.id).isFile) {
                serviceScope?.launch { runImport(operation.id, request, rethrow = false) }
            }
            return operation
        }
        val scope = serviceScope
        if (scope != null) {
            scope.launch { runImport(operation.id, request, rethrow = false) }
            return operation
        }
        return runImport(operation.id, request, rethrow = true)
    }

    fun recoverInterruptedImports() {
        val scope = serviceScope ?: return
        scope.launch(Dispatchers.IO) {
            runCatching(::cleanupInterruptedPublications).onFailure {
                android.util.Log.e(TAG, "Interrupted image publication cleanup failed class=${it::class.java.simpleName}")
            }
            if (!pendingImports.isDirectory) return@launch
            pendingImports.listFiles()?.filter { it.isFile && it.name.endsWith(".json") }?.sortedBy { it.name }
                ?.take(MAX_PENDING_IMPORTS)?.forEach { file ->
                    runCatching {
                        require(file.length() in 1..MAX_PENDING_FILE_BYTES) { "pending import record is out of bounds" }
                        val value = JSONObject(file.readText())
                        require(value.keys().asSequence().toSet() == setOf("operationId", "sourceUrl", "sha256", "expectedSizeBytes"))
                        val id = OperationId(value.getString("operationId"))
                        val request = ImageImportRequest(value.getString("sourceUrl"), value.getString("sha256"), value.getLong("expectedSizeBytes"))
                        val current = operations.load(id)
                        if (current == null || current.state.terminal) check(file.delete()) { "stale pending import could not be removed" }
                        else runImport(id, request, rethrow = false)
                    }.onFailure {
                        android.util.Log.e(TAG, "Pending image import recovery failed file=${file.name} class=${it::class.java.simpleName}")
                    }
                }
        }
    }

    private suspend fun runImport(operationId: OperationId, request: ImageImportRequest, rethrow: Boolean): OperationRecord = importEffectLock.withLock {
        try {
            lock.withLock {
                var latest = requireNotNull(operations.load(operationId))
                if (latest.state in setOf(OperationState.VERIFYING, OperationState.PREPARING_DISKS, OperationState.PREPARING_BOOT)) {
                    latest = update(latest, latest.transitionTo(OperationState.FAILED_RETRYABLE, "image.recover", "IMPORT_INTERRUPTED"))
                }
                if (latest.state == OperationState.FAILED_RETRYABLE) latest = update(latest, latest.transitionTo(OperationState.PREFLIGHT, "image.preflight"))
                if (latest.state == OperationState.ACCEPTED) latest = update(latest, latest.transitionTo(OperationState.PREFLIGHT, "image.preflight"))
                if (latest.state == OperationState.PREFLIGHT) update(latest, latest.transitionTo(OperationState.FETCHING, "image.fetch"))
            }
            val imageId = imageId(URI(request.sourceUrl))
            if (!isInstalled(imageId, request)) preflightSpace(request.expectedSizeBytes)
            downloadVerified(request, enrolledRepositoryOrigin(), imageId, operationId)
            cleanupInterruptedPublications()
            val completed = lock.withLock {
                val latest = requireNotNull(operations.load(operationId))
                if (latest.blocksArtifactEffects) latest else {
                    check(latest.state == OperationState.PREPARING_DISKS) { "image was not claimed for publication" }
                    val prepared = update(latest, latest.transitionTo(OperationState.PREPARING_BOOT, "image.publish"))
                    update(prepared, prepared.transitionTo(OperationState.SUCCEEDED, null))
                }
            }
            if (!pendingFile(operationId).delete() && pendingFile(operationId).exists()) {
                // Recovery removes terminal records; retain success rather than corrupting the operation transition.
                android.util.Log.w(TAG, "Completed image import record cleanup deferred operation=${operationId.value}")
            }
            completed
        } catch (failure: Throwable) {
            val failed = withContext(NonCancellable) {
                lock.withLock {
                    val latest = requireNotNull(operations.load(operationId))
                    if (latest.blocksArtifactEffects) latest
                    else update(latest, latest.transitionTo(OperationState.FAILED_RETRYABLE, "image.fetch", "IMAGE_IMPORT_FAILED"))
                }
            }
            if (failure is CancellationException || rethrow) throw failure
            failed
        }
    }

    private fun persistPending(id: OperationId, request: ImageImportRequest) {
        check(pendingImports.mkdirs() || pendingImports.isDirectory)
        require(pendingImports.listFiles()?.size.orZero() < MAX_PENDING_IMPORTS) { "pending import capacity exceeded" }
        val target = pendingFile(id)
        val temporary = File(pendingImports, ".${id.value}.tmp")
        FileOutputStream(temporary).use {
            it.write(JSONObject().put("operationId", id.value).put("sourceUrl", request.sourceUrl).put("sha256", request.sha256).put("expectedSizeBytes", request.expectedSizeBytes).toString().toByteArray())
            it.fd.sync()
        }
        check(temporary.renameTo(target)) { "pending import publication failed" }
    }

    private fun pendingFile(id: OperationId) = File(pendingImports, "${id.value}.json")
    private fun preflightSpace(expectedBytes: Long) {
        check(artifacts.mkdirs() || artifacts.isDirectory) { "artifact directory unavailable" }
        require(expectedBytes in 1..MAX_IMAGE_BYTES) { "image size is out of bounds" }
        require(artifacts.usableSpace >= expectedBytes + MIN_FREE_BYTES) { "insufficient free space for image import" }
        val retained = artifacts.walkTopDown().filter(File::isFile).take(MAX_ARTIFACT_FILES + 1).toList()
        require(retained.size <= MAX_ARTIFACT_FILES) { "artifact file quota exceeded" }
        require(retained.sumOf(File::length) + expectedBytes <= ARTIFACT_QUOTA_BYTES) { "artifact byte quota exceeded" }
    }

    private fun Int?.orZero() = this ?: 0

    override suspend fun removeVm(id: RuntimeId, idempotencyKey: String, canonicalRequest: ByteArray): OperationRecord = lock.withLock {
        require(id == RuntimeId.DEFAULT) { "MVP supports one runtime" }
        // Check the durable receipt before reading desired state: a lost DELETE response may
        // be retried after reconciliation has already removed the desired VM record.
        operations.operationForIdempotencyKey(idempotencyKey)?.let { existing ->
            require(existing.requestDigest == sha256(canonicalRequest)) {
                "idempotency key reused with different request"
            }
            return@withLock existing
        }
        val current = requireNotNull(operations.loadDesiredRuntime(id)) { "VM not found" }
        applyRuntime.apply(
            current.copy(generation = current.generation + 1, desiredState = DesiredRuntimeState.ABSENT),
            idempotencyKey,
            canonicalRequest,
        )
    }

    override suspend fun cancelOperation(id: String, idempotencyKey: String, canonicalRequest: ByteArray): OperationRecord = lock.withLock {
        require(idempotencyKey.length in 16..200) { "invalid idempotency key length" }
        require(canonicalRequest.isNotEmpty() && canonicalRequest.size <= MAX_CANONICAL_REQUEST_BYTES) { "canonical cancel request is out of bounds" }
        val digest = sha256(canonicalRequest)
        database.withTransaction {
            val db = database.openHelper.writableDatabase
            db.execSQL("CREATE TABLE IF NOT EXISTS cancel_operation_receipts (idempotencyKey TEXT PRIMARY KEY NOT NULL, requestDigest TEXT NOT NULL, targetOperationId TEXT NOT NULL, resultJson TEXT NOT NULL)")
            db.query("SELECT requestDigest,targetOperationId,resultJson FROM cancel_operation_receipts WHERE idempotencyKey = ?", arrayOf(idempotencyKey)).use { cursor ->
                if (cursor.moveToFirst()) {
                    require(cursor.getString(0) == digest && cursor.getString(1) == id) { "idempotency key reused with different cancel request" }
                    return@withTransaction JSONObject(cursor.getString(2)).operationRecord()
                }
            }
            val count = db.query("SELECT COUNT(*) FROM cancel_operation_receipts").use { it.moveToFirst(); it.getInt(0) }
            require(count < MAX_CANCEL_RECEIPTS) { "cancel receipt capacity exceeded" }
            val result = operations.cancelOperation(
                OperationId(id), OperationState.entries.filter { !it.terminal && it != OperationState.CANCELLING }.toSet(),
            )
            db.execSQL("INSERT INTO cancel_operation_receipts VALUES (?,?,?,?)", arrayOf<Any?>(idempotencyKey, digest, id, result.receiptJson().toString()))
            result
        }
    }

    private fun OperationRecord.receiptJson() = JSONObject()
        .put("id", id.value).put("idempotencyKey", idempotencyKey).put("requestDigest", requestDigest)
        .put("runtimeId", runtimeId?.value ?: JSONObject.NULL).put("desiredGeneration", desiredGeneration ?: JSONObject.NULL)
        .put("state", state.name).put("currentStepId", currentStepId ?: JSONObject.NULL).put("errorCode", errorCode ?: JSONObject.NULL)

    private fun JSONObject.operationRecord() = OperationRecord(
        OperationId(getString("id")), getString("idempotencyKey"), getString("requestDigest"),
        if (isNull("runtimeId")) null else getString("runtimeId").let(::RuntimeId),
        if (isNull("desiredGeneration")) null else getLong("desiredGeneration"), OperationState.valueOf(getString("state")),
        if (isNull("currentStepId")) null else getString("currentStepId"),
        if (isNull("errorCode")) null else getString("errorCode"),
    )

    override suspend fun revokeController(id: String, idempotencyKey: String, canonicalRequest: ByteArray) {
        // The MVP enrollment carries one controller. Revocation is fail-closed and durable.
        ControllerRevocations.revoke(database.openHelper.writableDatabase, id, idempotencyKey, sha256(canonicalRequest), clockMillis())
    }

    private suspend fun downloadVerified(request: ImageImportRequest, authority: URI, imageId: String, operationId: OperationId) = withContext(Dispatchers.IO) {
        check(artifacts.mkdirs() || artifacts.isDirectory) { "artifact directory unavailable" }
        if (isInstalled(imageId, request)) {
            lock.withLock {
                val latest = requireNotNull(operations.load(operationId))
                if (!latest.blocksArtifactEffects && latest.state == OperationState.FETCHING) {
                    val verified = update(latest, latest.transitionTo(OperationState.VERIFYING, "image.verify"))
                    update(verified, verified.transitionTo(OperationState.PREPARING_DISKS, "image.publish"))
                }
            }
            return@withContext
        }
        val temporary = File(artifacts, ".$imageId.${System.nanoTime()}.part")
        val deadline = SystemClock.elapsedRealtime() + DOWNLOAD_DEADLINE_MILLIS
        var current = URI(request.sourceUrl)
        try {
            FileOutputStream(temporary).use { output ->
                val digest = MessageDigest.getInstance("SHA-256")
                var redirects = 0
                while (true) {
                    validateArtifactUri(current, authority)
                    val validatedAddress = resolvePublicAddresses(current.host).first()
                    val remaining = deadline - SystemClock.elapsedRealtime()
                    check(remaining > 0) { "image download deadline exceeded" }
                    val addressLiteral = if (validatedAddress.address.size == 16) "[${validatedAddress.hostAddress}]" else validatedAddress.hostAddress
                    val connectedUrl = URL("https", addressLiteral, effectivePort(current), current.rawPath + (current.rawQuery?.let { "?$it" } ?: ""))
                    val connection = connectedUrl.openConnection() as HttpsURLConnection
                    connection.hostnameVerifier = javax.net.ssl.HostnameVerifier { _, session ->
                        HttpsURLConnection.getDefaultHostnameVerifier().verify(current.host, session)
                    }
                    connection.sslSocketFactory = SniSocketFactory(current.host, HttpsURLConnection.getDefaultSSLSocketFactory())
                    connection.setRequestProperty("Host", current.host + if (effectivePort(current) == 443) "" else ":${effectivePort(current)}")
                    connection.instanceFollowRedirects = false
                    connection.connectTimeout = minOf(10_000L, remaining).toInt()
                    connection.readTimeout = minOf(15_000L, remaining).toInt()
                    connection.requestMethod = "GET"
                    connection.connect()
                    try {
                        if (connection.responseCode in 300..399) {
                            require(++redirects <= MAX_REDIRECTS) { "too many image redirects" }
                            current = current.resolve(requireNotNull(connection.getHeaderField("Location")) { "redirect missing location" })
                            continue
                        }
                        require(connection.responseCode == 200) { "image server returned ${connection.responseCode}" }
                        val declared = connection.contentLengthLong
                        require(declared == -1L || declared == request.expectedSizeBytes) { "image size header mismatch" }
                        connection.inputStream.use { input ->
                            val buffer = ByteArray(BUFFER_BYTES)
                            var total = 0L
                            while (true) {
                                currentCoroutineContext().ensureActive()
                                check(!operationCancelled(operationId)) { "image import cancelled" }
                                check(SystemClock.elapsedRealtime() < deadline) { "image download deadline exceeded" }
                                val count = input.read(buffer)
                                if (count < 0) break
                                total += count
                                require(total <= request.expectedSizeBytes) { "image exceeds expected size" }
                                output.write(buffer, 0, count)
                                digest.update(buffer, 0, count)
                            }
                            require(total == request.expectedSizeBytes) { "image size mismatch" }
                        }
                        require(digest.digest().joinToString("") { "%02x".format(it) } == request.sha256) { "image digest mismatch" }
                        output.flush(); output.fd.sync()
                        break
                    } finally { connection.disconnect() }
                }
            }
            publishVerifiedDownload(operationId, imageId, request, temporary)
        } finally { temporary.delete() }
    }

    /** Cancellation is sampled under the same lock as the durable publication claim. */
    internal suspend fun publishVerifiedDownload(
        operationId: OperationId,
        imageId: String,
        request: ImageImportRequest,
        temporary: File,
    ): Boolean {
        beforePublicationClaim(operationId)
        val claimed = lock.withLock {
            val latest = requireNotNull(operations.load(operationId))
            if (latest.blocksArtifactEffects) false else {
                check(latest.state == OperationState.FETCHING)
                val verified = update(latest, latest.transitionTo(OperationState.VERIFYING, "image.verify"))
                update(verified, verified.transitionTo(OperationState.PREPARING_DISKS, "image.publish"))
                true
            }
        }
        if (!claimed) return false
        // PREPARING_DISKS is the durable non-cancellable publication claim. Cancellation can no longer win.
        val versionPayload = File(artifacts, "versions/$imageId/${request.sha256}/payload")
        val versionDirectory = requireNotNull(versionPayload.parentFile)
        check(versionDirectory.mkdirs() || versionDirectory.isDirectory)
        java.nio.file.Files.move(
            temporary.toPath(), versionPayload.toPath(),
            java.nio.file.StandardCopyOption.ATOMIC_MOVE, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
        )
        val manifest = File(artifacts, "$imageId.manifest.json")
        val manifestTemporary = File(artifacts, ".$imageId.manifest.part")
        try {
            val manifestValue = JSONObject().put("version", 1).put("sha256", request.sha256)
                .put("sizeBytes", request.expectedSizeBytes).put("relativePath", "versions/$imageId/${request.sha256}/payload")
            FileOutputStream(manifestTemporary).use { it.write(manifestValue.toString().toByteArray()); it.fd.sync() }
            java.nio.file.Files.move(
                manifestTemporary.toPath(), manifest.toPath(),
                java.nio.file.StandardCopyOption.ATOMIC_MOVE, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
            )
        } finally { manifestTemporary.delete() }
        return true
    }

    internal fun cleanupInterruptedPublications() {
        if (!artifacts.isDirectory) return
        artifacts.walkTopDown().filter(File::isFile).take(MAX_ARTIFACT_FILES + 1).forEach { file ->
            val relative = file.relativeTo(artifacts).invariantSeparatorsPath
            val temporary = file.parentFile == artifacts && file.name.startsWith('.') && file.name.endsWith(".part")
            val payload = PAYLOAD_PATH.matchEntire(relative)
            val orphanedPayload = payload != null && run {
                val imageId = payload.groupValues[1]
                val digest = payload.groupValues[2]
                val manifest = File(artifacts, "$imageId.manifest.json")
                val value = manifest.takeIf { it.isFile && it.length() in 1..MAX_MANIFEST_BYTES }
                    ?.let { runCatching { JSONObject(it.readText()) }.getOrNull() }
                value?.optInt("version") != 1 || value.optString("sha256") != digest ||
                    value.optString("relativePath") != relative
            }
            if (temporary || orphanedPayload) check(file.delete()) { "interrupted artifact could not be removed" }
        }
        File(artifacts, "versions").walkBottomUp().filter(File::isDirectory).forEach { directory ->
            if (directory != File(artifacts, "versions") && directory.list()?.isEmpty() == true) directory.delete()
        }
    }

    private fun isInstalled(imageId: String, request: ImageImportRequest): Boolean {
        val manifest = File(artifacts, "$imageId.manifest.json")
        val payload = File(artifacts, "versions/$imageId/${request.sha256}/payload")
        if (!manifest.isFile || manifest.length() !in 1..MAX_MANIFEST_BYTES || !payload.isFile || payload.length() != request.expectedSizeBytes) return false
        val value = runCatching { JSONObject(manifest.readText()) }.getOrNull() ?: return false
        if (value.optInt("version") != 1 || value.optString("sha256") != request.sha256 ||
            value.optLong("sizeBytes") != request.expectedSizeBytes ||
            value.optString("relativePath") != "versions/$imageId/${request.sha256}/payload"
        ) return false
        return sha256(payload) == request.sha256
    }

    private fun operationCancelled(id: OperationId): Boolean =
        database.openHelper.readableDatabase.query("SELECT state FROM operations WHERE id = ?", arrayOf(id.value)).use {
            it.moveToFirst() && it.getString(0) == OperationState.CANCELLED.name
        }

    private fun validateArtifactUri(uri: URI, authority: URI) {
        require(uri.scheme == "https" && uri.userInfo == null && uri.fragment == null && uri.host != null) { "invalid artifact URL" }
        require(uri.host.equals(authority.host, true) && effectivePort(uri) == effectivePort(authority)) {
            "artifact URL is outside enrolled repository origin"
        }
        val enrolledPath = authority.path.orEmpty().trimEnd('/')
        require(enrolledPath.isEmpty() || uri.path == enrolledPath || uri.path.startsWith("$enrolledPath/")) {
            "artifact URL is outside enrolled repository path"
        }
    }
    internal fun resolvePublicAddresses(host: String): List<InetAddress> {
        // Resolve exactly once for this hop. The selected literal is used for connect, preventing rebinding.
        val addresses = addressResolver(host)
        require(addresses.isNotEmpty() && addresses.size <= 16) { "artifact host address count is out of bounds" }
        require(addresses.all(::isGloballyRoutable)) { "artifact host resolved to a non-global address" }
        return addresses
    }

    internal fun isGloballyRoutable(address: InetAddress): Boolean {
        val bytes = address.address.map { it.toInt() and 0xff }
        if (bytes.size == 4) {
            val a = bytes[0]; val b = bytes[1]; val c = bytes[2]
            return when {
                a == 0 || a == 10 || a == 127 || a >= 224 -> false
                a == 100 && b in 64..127 -> false // RFC 6598 CGNAT
                a == 169 && b == 254 -> false
                a == 172 && b in 16..31 -> false
                a == 192 && b == 0 -> false
                a == 192 && b == 168 -> false
                a == 192 && b == 88 && c == 99 -> false
                a == 198 && b in setOf(18, 19, 51) -> false
                a == 203 && b == 0 && c == 113 -> false
                else -> true
            }
        }
        if (bytes.size != 16 || bytes[0] !in 0x20..0x3f) return false // only IPv6 global unicast 2000::/3
        if (bytes[0] == 0x20 && bytes[1] == 0x01 && bytes[2] == 0x0d && bytes[3] == 0xb8) return false
        return !address.isAnyLocalAddress && !address.isLoopbackAddress && !address.isLinkLocalAddress && !address.isMulticastAddress
    }
    private fun imageId(uri: URI): String = uri.path.substringAfterLast('/').also {
        require(Regex("[a-z0-9][a-z0-9.-]{0,127}").matches(it)) { "artifact URL must end in a typed image id" }
    }
    private fun effectivePort(uri: URI) = if (uri.port >= 0) uri.port else 443
    private suspend fun existing(key: String) = database.dao().operationByKey(key)?.toRecord()
    private suspend fun insert(record: OperationRecord) = database.withTransaction {
        require(database.dao().operationCount() < RoomOperationRepository.MAX_RETAINED_OPERATIONS) { "operation journal capacity exceeded" }
        database.dao().insertOperation(record.toEntity(clockMillis()))
    }
    private suspend fun update(expected: OperationRecord, updated: OperationRecord): OperationRecord {
        check(operations.compareAndSetOperation(expected, updated)) { "operation changed concurrently" }
        return updated
    }
    private fun OperationRecord.toEntity(created: Long) = OperationEntity(id.value, idempotencyKey, requestDigest, runtimeId?.value, desiredGeneration, state.name, currentStepId, errorCode, created, clockMillis())
    private fun OperationEntity.toRecord() = OperationRecord(OperationId(id), idempotencyKey, requestDigest, runtimeId?.let(::RuntimeId), desiredGeneration, OperationState.valueOf(state), currentStepId, errorCode)
    private fun sha256(value: ByteArray) = MessageDigest.getInstance("SHA-256").digest(value).joinToString("") { "%02x".format(it) }
    private fun sha256(file: File): String {
        val digest = MessageDigest.getInstance("SHA-256")
        file.inputStream().use { input ->
            val buffer = ByteArray(BUFFER_BYTES)
            while (true) {
                val count = input.read(buffer)
                if (count < 0) break
                digest.update(buffer, 0, count)
            }
        }
        return digest.digest().joinToString("") { "%02x".format(it) }
    }

    private class SniSocketFactory(
        private val tlsHostname: String,
        private val delegate: SSLSocketFactory,
    ) : SSLSocketFactory() {
        override fun getDefaultCipherSuites() = delegate.defaultCipherSuites
        override fun getSupportedCipherSuites() = delegate.supportedCipherSuites
        override fun createSocket(socket: java.net.Socket, host: String, port: Int, autoClose: Boolean) = configure(delegate.createSocket(socket, host, port, autoClose))
        override fun createSocket(host: String, port: Int) = configure(delegate.createSocket(host, port))
        override fun createSocket(host: String, port: Int, local: InetAddress, localPort: Int) = configure(delegate.createSocket(host, port, local, localPort))
        override fun createSocket(host: InetAddress, port: Int) = configure(delegate.createSocket(host, port))
        override fun createSocket(host: InetAddress, port: Int, local: InetAddress, localPort: Int) = configure(delegate.createSocket(host, port, local, localPort))
        private fun configure(socket: java.net.Socket): java.net.Socket = socket.apply {
            if (this is SSLSocket) sslParameters = sslParameters.apply { serverNames = listOf(SNIHostName(tlsHostname)) }
        }
    }

    private companion object {
        const val BUFFER_BYTES = 64 * 1024
        const val DOWNLOAD_DEADLINE_MILLIS = 15 * 60 * 1000L
        const val MAX_REDIRECTS = 5
        const val TAG = "NodeHostImages"
        const val MAX_PENDING_IMPORTS = 16
        const val MAX_PENDING_FILE_BYTES = 4096L
        val PAYLOAD_PATH = Regex("versions/([a-z0-9][a-z0-9.-]{0,127})/([a-f0-9]{64})/payload")
        const val MAX_ARTIFACT_FILES = 512
        const val MAX_IMAGE_BYTES = 64L * 1024 * 1024 * 1024
        const val ARTIFACT_QUOTA_BYTES = 96L * 1024 * 1024 * 1024
        const val MIN_FREE_BYTES = 256L * 1024 * 1024
        const val MAX_MANIFEST_BYTES = 4096L
        const val MAX_CANONICAL_REQUEST_BYTES = 64 * 1024
        const val MAX_CANCEL_RECEIPTS = 256
    }
}

internal object ControllerRevocations {
    fun revoke(db: androidx.sqlite.db.SupportSQLiteDatabase, controllerId: String, key: String, digest: String, now: Long) {
        db.execSQL("CREATE TABLE IF NOT EXISTS controller_revocations (controllerId TEXT PRIMARY KEY NOT NULL, idempotencyKey TEXT NOT NULL UNIQUE, requestDigest TEXT NOT NULL, revokedAtEpochMillis INTEGER NOT NULL)")
        db.query("SELECT requestDigest FROM controller_revocations WHERE idempotencyKey = ?", arrayOf(key)).use {
            if (it.moveToFirst()) { require(it.getString(0) == digest) { "idempotency key reused with different request" }; return }
        }
        db.execSQL("INSERT INTO controller_revocations VALUES (?,?,?,?)", arrayOf<Any?>(controllerId, key, digest, now))
    }
    fun isRevoked(db: androidx.sqlite.db.SupportSQLiteDatabase, controllerId: String): Boolean {
        db.execSQL("CREATE TABLE IF NOT EXISTS controller_revocations (controllerId TEXT PRIMARY KEY NOT NULL, idempotencyKey TEXT NOT NULL UNIQUE, requestDigest TEXT NOT NULL, revokedAtEpochMillis INTEGER NOT NULL)")
        return db.query("SELECT 1 FROM controller_revocations WHERE controllerId = ? LIMIT 1", arrayOf(controllerId)).use { it.moveToFirst() }
    }
}
