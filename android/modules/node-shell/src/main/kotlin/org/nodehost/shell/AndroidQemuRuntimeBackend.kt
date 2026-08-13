package org.nodehost.shell

import android.content.Context
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.RandomAccessFile
import java.security.MessageDigest
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.nodehost.core.OperationContext
import org.nodehost.core.RuntimeBackend
import org.nodehost.core.RuntimeStep
import org.nodehost.core.StepOutcome
import org.nodehost.model.ArtifactRef
import org.nodehost.model.BootSpec
import org.nodehost.model.DataDiskSpec
import org.nodehost.model.DesiredRuntimeState
import org.nodehost.model.DiskFormat
import org.nodehost.model.HealthKind
import org.nodehost.model.HealthSpec
import org.nodehost.model.InitializationKind
import org.nodehost.model.InitializationSpec
import org.nodehost.model.MachineSpec
import org.nodehost.model.ProfileRequirements
import org.nodehost.model.RecoverySshSpec
import org.nodehost.model.RuntimeId
import org.nodehost.model.RuntimeObservation
import org.nodehost.model.RuntimeSpec
import org.nodehost.model.SystemDiskSpec
import org.nodehost.model.VmProfile
import org.nodehost.model.VmProfileId
import org.nodehost.model.WritableLayer
import org.nodehost.qemu.BootstrapToken
import org.nodehost.qemu.QemuExit
import org.nodehost.qemu.QemuLaunchPlan
import org.nodehost.qemu.QemuProfileResolver
import org.nodehost.qemu.QemuRuntimeAdapter
import org.nodehost.qemu.QemuRuntimeAllocation
import org.nodehost.qemu.RecoverySshHostPort
import org.json.JSONObject

internal data class ManagedQemuProcess(
    val processId: Long?,
    val awaitExit: suspend () -> QemuExit,
    val requestGuestShutdown: suspend () -> Unit,
    val awaitConsoleMarker: suspend (String, Long) -> Unit = { _, _ ->
        error("console-marker health is unavailable")
    },
)

internal interface QemuProcessControl {
    suspend fun start(plan: QemuLaunchPlan, runtime: RuntimeSpec): ManagedQemuProcess
    fun forceStop()
}

private class RuntimeQemuProcessControl(private val adapter: QemuRuntimeAdapter = QemuRuntimeAdapter()) : QemuProcessControl {
    override suspend fun start(plan: QemuLaunchPlan, runtime: RuntimeSpec): ManagedQemuProcess {
        val handle = adapter.start(plan)
        return ManagedQemuProcess(
            handle.processId,
            { adapter.awaitExit(handle) },
            { adapter.requestGuestShutdown(handle) },
            { marker, timeoutMillis -> adapter.awaitConsoleMarker(handle, marker, timeoutMillis) },
        )
    }
    override fun forceStop() = adapter.forceStop()
}

/** RuntimeBackend adapter whose only process effect is the typed runtime-qemu API. */
internal class AndroidQemuRuntimeBackend(
    context: Context,
    private val desiredRuntime: suspend () -> RuntimeSpec?,
    private val beginBootToken: suspend (VmProfileId) -> String?,
    private val recoveryPort: RecoverySshHostPort,
    private val qemu: QemuProcessControl = RuntimeQemuProcessControl(),
    private val artifactTrust: NodeHostArtifactTrust? = null,
    private val elapsedRealtimeMillis: () -> Long = android.os.SystemClock::elapsedRealtime,
    private val gracefulStopMillis: Long = GRACEFUL_STOP_MILLIS,
    private val forceExitMillis: Long = FORCE_EXIT_MILLIS,
    private val atomicMove: (File, File) -> Unit = { source, target ->
        Files.move(source.toPath(), target.toPath(), StandardCopyOption.ATOMIC_MOVE)
    },
) : RuntimeBackend {
    private val application = context.applicationContext
    private val bundledArtifactTrust by lazy {
        artifactTrust ?: NodeHostArtifactTrust.fromApplication(application)
    }
    private val artifactRoot = File(application.filesDir, "nodehost-artifacts")
    private val resolver = QemuProfileResolver()
    private val stateLock = Any()
    private var handle: ManagedQemuProcess? = null
    private var preparedLaunch: Pair<QemuLaunchPlan, RuntimeSpec>? = null
    private var profileId: VmProfileId? = null
    private var appliedGeneration: Long? = null
    private var qmpReady = false
    private var guestReady = false
    private var stopping = false
    private var gracefulDeadlineElapsedRealtime: Long? = null
    private var exitWatcher: Job? = null
    private var serviceScope: CoroutineScope? = null
    private var wakeReconciler: (() -> Unit)? = null

    /** Service-owned observation is attached after composition so process exit always wakes convergence. */
    fun attachLifecycle(scope: CoroutineScope, wake: () -> Unit) = synchronized(stateLock) {
        serviceScope = scope
        wakeReconciler = wake
        handle?.let(::watchExitLocked)
        if (stopping) handle?.let(::scheduleGracefulDeadlineWake)
    }

    override suspend fun observe(id: RuntimeId): RuntimeObservation = synchronized(stateLock) {
        require(id == RuntimeId.DEFAULT) { "MVP supports one runtime" }
        when {
            handle != null && stopping -> RuntimeObservation.Stopping(
                id, handle?.processId,
                gracefulDeadlineElapsedRealtime?.let { elapsedRealtimeMillis() >= it } == true,
            )
            handle != null && qmpReady -> RuntimeObservation.Running(
                id, handle?.processId, guestReady,
                checkNotNull(appliedGeneration) { "running QEMU process has no applied generation" },
            )
            handle != null -> RuntimeObservation.Starting(id, handle?.processId)
            instanceDirectory().isDirectory -> RuntimeObservation.Stopped(id, profileId)
            else -> RuntimeObservation.Absent(id)
        }
    }

    override suspend fun execute(context: OperationContext, step: RuntimeStep): StepOutcome {
        val desired = desiredRuntime() ?: error("desired runtime disappeared")
        require(desired.id == RuntimeId.DEFAULT) { "MVP supports one runtime" }
        return when (step) {
            RuntimeStep.VerifyProfile -> {
                require(desired.desiredState == DesiredRuntimeState.RUNNING)
                profile(desired.profileId, verifyArtifacts = true)
                StepOutcome(false, "profile=${desired.profileId.value}")
            }
            RuntimeStep.ResolveArtifacts -> {
                profile(desired.profileId, verifyArtifacts = true)
                StepOutcome(false, "artifacts-verified")
            }
            RuntimeStep.PrepareDisks -> withContext(Dispatchers.IO) {
                prepareDisks(desired)
                StepOutcome(true, "disks-prepared")
            }
            RuntimeStep.PrepareBoot -> {
                File(instanceDirectory(), "guest-ready").delete()
                val token = beginBootToken(desired.profileId)?.let(::BootstrapToken)
                val allocation = QemuRuntimeAllocation.fromApplication(application, recoveryPort, token)
                val resolved = resolver.resolve(profile(desired.profileId, verifyArtifacts = true), desired, allocation)
                synchronized(stateLock) { preparedLaunch = resolved to desired; profileId = desired.profileId }
                StepOutcome(true, "boot-prepared")
            }
            RuntimeStep.StartProcess -> {
                val prepared = synchronized(stateLock) { checkNotNull(preparedLaunch) { "boot is not prepared" } }
                val started = qemu.start(prepared.first, prepared.second)
                synchronized(stateLock) {
                    handle = started
                    appliedGeneration = prepared.second.generation
                    qmpReady = false
                    guestReady = false
                    stopping = false
                    gracefulDeadlineElapsedRealtime = null
                    watchExitLocked(started)
                }
                StepOutcome(true, "qemu-started")
            }
            RuntimeStep.WaitForQmp -> {
                awaitFile(File(instanceDirectory(), "qmp.sock"), QMP_WAIT_MILLIS)
                synchronized(stateLock) {
                    checkNotNull(handle)
                    qmpReady = true
                }
                StepOutcome(false, "qmp-ready")
            }
            RuntimeStep.WaitForGuest -> {
                val active = synchronized(stateLock) { checkNotNull(handle) }
                val health = profile(requireNotNull(profileId), verifyArtifacts = false).health
                when (health.kind) {
                    HealthKind.CONSOLE_MARKER ->
                        active.awaitConsoleMarker(requireNotNull(health.marker), GUEST_WAIT_MILLIS)
                    HealthKind.METADATA_CALLBACK ->
                        awaitFile(File(instanceDirectory(), "guest-ready"), GUEST_WAIT_MILLIS)
                    HealthKind.SSH -> error("SSH health is not implemented for this profile")
                }
                synchronized(stateLock) { checkNotNull(handle); guestReady = true }
                StepOutcome(false, "guest-ready")
            }
            RuntimeStep.RequestShutdown -> {
                val (active, firstShutdownRequest) = synchronized(stateLock) {
                    val current = handle
                    val first = current != null && !stopping
                    if (first) {
                        stopping = true
                        gracefulDeadlineElapsedRealtime = elapsedRealtimeMillis() + gracefulStopMillis
                    }
                    current to first
                }
                // Install this before QMP. A retryable QMP failure must not erase the original
                // deadline or leave the actor waiting on a failed request with no wake.
                if (firstShutdownRequest) active?.let(::scheduleGracefulDeadlineWake)
                try {
                    if (active != null) {
                        withTimeout(gracefulStopMillis.coerceAtMost(QMP_SHUTDOWN_TIMEOUT_MILLIS)) {
                            active.requestGuestShutdown()
                        }
                    }
                } finally {
                    if (active == null) {
                        synchronized(stateLock) {
                            gracefulDeadlineElapsedRealtime = null
                            stopping = false
                        }
                    }
                }
                StepOutcome(active != null, "shutdown-requested")
            }
            RuntimeStep.ForceStop -> {
                val active = synchronized(stateLock) { handle }
                if (active != null) {
                    qemu.forceStop()
                    try {
                        withTimeout(forceExitMillis) { active.awaitExit() }
                    } finally {
                        if (synchronized(stateLock) { handle === active }) {
                            serviceScope?.launch { delay(POLL_MILLIS); wakeReconciler?.invoke() }
                        }
                    }
                }
                StepOutcome(active != null, "qemu-force-stopped")
            }
            RuntimeStep.RemoveSystem -> withContext(Dispatchers.IO) {
                check(synchronized(stateLock) { handle == null }) { "cannot remove a running runtime" }
                val instance = instanceDirectory()
                val diskName = persistentDiskName(desired.profileId)
                val data = File(instance, diskName)
                val durable = durableDataFile(desired.profileId)
                if (desired.preserveDataOnDelete && data.exists()) {
                    check(durable.parentFile?.mkdirs() == true || durable.parentFile?.isDirectory == true) { "durable data directory unavailable" }
                    check(!durable.exists()) { "durable data disk already exists" }
                    atomicMove(data, durable) // Failure aborts before any system deletion.
                    check(durable.isFile && !data.exists()) { "data disk preservation was not atomic" }
                } else if (!desired.preserveDataOnDelete && durable.exists()) {
                    check(durable.delete()) { "preserved data disk could not be discarded" }
                }
                deleteBounded(instance, MAX_DELETE_ENTRIES)
                synchronized(stateLock) {
                    preparedLaunch = null
                    profileId = null
                    appliedGeneration = null
                }
                StepOutcome(true, "system-removed")
            }
        }
    }

    internal fun profile(id: VmProfileId, verifyArtifacts: Boolean): VmProfile = when (id.value) {
        ALPINE_PROFILE -> VmProfile(
            id, 1, machine = MachineSpec(cpuModel = "max"),
            boot = BootSpec.DirectKernel(artifact("podroid-kernel", verifyArtifacts), artifact("podroid-initramfs", verifyArtifacts), "podroid-compatible-v1"),
            systemDisk = SystemDiskSpec(artifact("podroid-alpine-squashfs", verifyArtifacts), DiskFormat.SQUASHFS, WritableLayer.SEPARATE_EXT4_OVERLAY),
            dataDisk = DataDiskSpec(4, true),
            initialization = InitializationSpec(InitializationKind.LEGACY_PODROID, "guest-init/alpine-direct/vendor-data.yaml"),
            recoverySsh = RecoverySshSpec(), health = HealthSpec(HealthKind.CONSOLE_MARKER, "Ready!"),
            requirements = ProfileRequirements(512, 3, setOf("virtio-block", "virtio-net", "serial-console", "overlayfs")),
        )
        UBUNTU_PROFILE, K3S_PROFILE -> {
            val k3s = id.value == K3S_PROFILE
            VmProfile(
                id, 1, extends = if (k3s) VmProfileId(UBUNTU_PROFILE) else null,
                machine = MachineSpec(cpuModel = "max"),
                boot = BootSpec.Uefi(artifact("aavmf-code", verifyArtifacts), artifact("aavmf-vars", verifyArtifacts)),
                systemDisk = SystemDiskSpec(artifact("ubuntu-2404-arm64-cloud", verifyArtifacts), DiskFormat.QCOW2, WritableLayer.QCOW2_OVERLAY),
                dataDisk = DataDiskSpec(8, true),
                initialization = InitializationSpec(InitializationKind.NOCLOUD_NET, if (k3s) "guest-init/k3s-worker-lab/vendor-data.yaml" else "guest-init/ubuntu/vendor-data.yaml", "/v1/bootstrap/{token}/"),
                recoverySsh = RecoverySshSpec(), health = HealthSpec(HealthKind.METADATA_CALLBACK),
                requirements = if (k3s) ProfileRequirements(1024, 8, K3S_CHECKS) else ProfileRequirements(768, 5, setOf("uefi", "cloud-init", "openssh")),
            )
        }
        else -> error("unsupported profile: ${id.value}")
    }

    private fun artifact(id: String, verify: Boolean): ArtifactRef {
        val file = artifactFile(id)
        if (isBundledArtifact(id)) {
            val trusted = bundledArtifactTrust.verifyFile(id, file)
            return ArtifactRef(id, trusted.sha256, trusted.sizeBytes)
        }
        require(file.isFile && file.length() in 1..MAX_ARTIFACT_BYTES) { "trusted artifact is missing or out of bounds: $id" }
        val digest = sha256(file)
        if (verify) require(expectedDigest(id) == digest) { "artifact digest mismatch: $id" }
        return ArtifactRef(id, digest, file.length())
    }

    private fun artifactFile(id: String): File {
        if (isBundledArtifact(id)) return requireNotNull(bundledArtifactFile(id))
        val manifest = File(artifactRoot, "$id.manifest.json")
        if (manifest.isFile) {
            require(manifest.length() in 1..4096) { "artifact manifest is out of bounds: $id" }
            val relative = JSONObject(manifest.readText()).getString("relativePath")
            val resolved = File(artifactRoot, relative).canonicalFile
            require(resolved.path.startsWith(artifactRoot.canonicalPath + File.separator)) { "artifact manifest escaped its root" }
            return resolved
        }
        val imported = File(artifactRoot, id)
        if (imported.isFile) return imported
        // Podroid extracts the signed Alpine bundle into filesDir, while imported artifacts
        // live under nodehost-artifacts. Keep both sources explicit and instance preparation
        // can consume the bundled profile without requiring a controller upload first.
        return bundledArtifactFile(id) ?: imported
    }

    private fun bundledArtifactFile(id: String): File? = when (id) {
        "podroid-kernel" -> File(application.filesDir, "vmlinuz-virt")
        "podroid-initramfs" -> File(application.filesDir, "initrd.img")
        "podroid-alpine-squashfs" -> File(application.filesDir, "alpine-rootfs.squashfs")
        else -> null
    }

    private fun expectedDigest(id: String): String {
        if (isBundledArtifact(id)) return bundledArtifactTrust.artifact(id).sha256
        val manifest = File(artifactRoot, "$id.manifest.json")
        if (manifest.isFile) return JSONObject(manifest.readText()).getString("sha256")
        val expected = File(artifactRoot, "$id.sha256")
        if (expected.isFile) {
            require(expected.length() <= 128) { "artifact digest metadata is out of bounds: $id" }
            return expected.readText().trim()
        }
        error("artifact digest metadata is missing: $id")
    }

    private fun isBundledArtifact(id: String): Boolean =
        id == "podroid-kernel" || id == "podroid-initramfs" || id == "podroid-alpine-squashfs"

    private fun prepareDisks(runtime: RuntimeSpec) {
        val instance = instanceDirectory().apply { check(mkdirs() || isDirectory) }
        val artifacts = File(instance, "artifacts").apply { check(mkdirs() || isDirectory) }
        when (runtime.profileId.value) {
            ALPINE_PROFILE -> {
                copyVerified("podroid-kernel", File(artifacts, "vmlinuz-virt"))
                copyVerified("podroid-initramfs", File(artifacts, "initrd.img"))
                copyVerified("podroid-alpine-squashfs", File(artifacts, "alpine-rootfs.squashfs"))
                val overlay = File(instance, "storage.img")
                val durable = durableDataFile(runtime.profileId)
                if (!overlay.exists() && durable.exists()) {
                    check(atomicMove(durable, overlay).let { overlay.isFile && !durable.exists() }) {
                        "data disk restoration was not atomic"
                    }
                }
                if (!overlay.exists()) RandomAccessFile(overlay, "rw").use { it.setLength(runtime.dataDiskGiB * GIB) }
            }
            UBUNTU_PROFILE, K3S_PROFILE -> {
                copyVerified("aavmf-code", File(artifacts, "AAVMF_CODE.fd"))
                // These are mutable VM state. Verify the imported source, but create each target exactly once.
                copyVerifiedOnce("aavmf-vars", File(instance, "firmware-vars.fd"))
                copyVerifiedOnce("ubuntu-2404-arm64-cloud", File(instance, "system.qcow2"))
                val data = File(instance, "data.raw")
                val durable = durableDataFile(runtime.profileId)
                if (!data.exists() && durable.exists()) {
                    atomicMove(durable, data)
                    check(data.isFile && !durable.exists()) { "data disk restoration was not atomic" }
                }
                if (!data.exists()) RandomAccessFile(data, "rw").use { it.setLength(runtime.dataDiskGiB * GIB) }
            }
            else -> error("unsupported profile: ${runtime.profileId.value}")
        }
    }

    private fun copyVerifiedOnce(id: String, target: File) {
        artifact(id, true)
        if (target.exists()) {
            require(target.isFile && target.length() in 1..MAX_ARTIFACT_BYTES) { "mutable system state is invalid: ${target.name}" }
            return
        }
        copyVerified(id, target)
    }

    private fun copyVerified(id: String, target: File) {
        val source = artifactFile(id)
        artifact(id, true)
        if (target.isFile && target.length() == source.length() && sha256(target) == sha256(source)) return
        val temporary = File(target.parentFile, target.name + ".tmp")
        FileInputStream(source).use { input -> FileOutputStream(temporary).use { output -> input.copyTo(output, COPY_BUFFER_BYTES); output.fd.sync() } }
        check(temporary.renameTo(target)) { "atomic artifact publication failed: $id" }
    }

    private suspend fun awaitFile(file: File, timeoutMillis: Long) {
        val end = elapsedRealtimeMillis() + timeoutMillis
        while (!file.exists()) {
            check(synchronized(stateLock) { handle != null }) { "QEMU exited before runtime readiness: ${file.name}" }
            check(elapsedRealtimeMillis() < end) { "runtime readiness deadline exceeded: ${file.name}" }
            delay(POLL_MILLIS)
        }
    }

    private fun scheduleGracefulDeadlineWake(active: ManagedQemuProcess) {
        val scope = serviceScope ?: return
        scope.launch {
            val deadline = synchronized(stateLock) {
                if (handle === active && stopping) gracefulDeadlineElapsedRealtime else null
            } ?: return@launch
            delay((deadline - elapsedRealtimeMillis()).coerceAtLeast(0L))
            synchronized(stateLock) {
                if (handle === active && stopping) wakeReconciler?.invoke()
            }
        }
    }

    private fun watchExitLocked(started: ManagedQemuProcess) {
        val scope = serviceScope ?: return
        exitWatcher?.cancel()
        exitWatcher = scope.launch {
            val result = runCatching { started.awaitExit() }
            result.onSuccess { android.util.Log.i(TAG, "QEMU exited code=${it.code}") }
                .onFailure { android.util.Log.e(TAG, "QEMU exit observation failed class=${it::class.java.simpleName}") }
            synchronized(stateLock) {
                if (handle === started) {
                    handle = null
                    appliedGeneration = null
                    qmpReady = false
                    guestReady = false
                    stopping = false
                    gracefulDeadlineElapsedRealtime = null
                    wakeReconciler?.invoke()
                }
            }
        }
    }

    private fun instanceDirectory() = File(application.filesDir, "vms/default")

    private fun persistentDiskName(profileId: VmProfileId): String = when (profileId.value) {
        ALPINE_PROFILE -> "storage.img"
        UBUNTU_PROFILE, K3S_PROFILE -> "data.raw"
        else -> error("unsupported profile: ${profileId.value}")
    }

    private fun durableDataFile(profileId: VmProfileId): File =
        File(application.filesDir, "nodehost-durable/vms/default/${persistentDiskName(profileId)}")

    private fun sha256(file: File): String {
        val digest = MessageDigest.getInstance("SHA-256")
        FileInputStream(file).use { input ->
            val buffer = ByteArray(COPY_BUFFER_BYTES)
            while (true) {
                val count = input.read(buffer)
                if (count < 0) break
                digest.update(buffer, 0, count)
            }
        }
        return digest.digest().joinToString("") { "%02x".format(it) }
    }

    private fun deleteBounded(root: File, remaining: Int): Int {
        if (!root.exists()) return remaining
        require(remaining > 0) { "runtime deletion exceeded entry bound" }
        var budget = remaining - 1
        root.listFiles()?.forEach { budget = deleteBounded(it, budget) }
        check(root.delete()) { "failed to delete runtime path" }
        return budget
    }

    private companion object {
        const val TAG = "NodeHostQemu"
        const val ALPINE_PROFILE = "alpine-direct-qualification"
        const val UBUNTU_PROFILE = "ubuntu-2404-arm64-uefi"
        const val K3S_PROFILE = "k3s-worker-lab"
        val K3S_CHECKS = setOf("cgroup-v2", "namespaces", "overlayfs", "br-netfilter", "vxlan", "tun", "iptables-or-nft", "ip-forwarding", "swap-policy", "minimum-memory", "minimum-storage", "tailscale-reachability")
        const val COPY_BUFFER_BYTES = 1024 * 1024
        const val MAX_ARTIFACT_BYTES = 64L * 1024 * 1024 * 1024
        const val MAX_DELETE_ENTRIES = 4096
        const val QMP_WAIT_MILLIS = 10_000L
        const val GUEST_WAIT_MILLIS = 25_000L
        const val POLL_MILLIS = 100L
        const val GRACEFUL_STOP_MILLIS = 20_000L
        const val FORCE_EXIT_MILLIS = 5_000L
        const val QMP_SHUTDOWN_TIMEOUT_MILLIS = 3_000L
        const val GIB = 1024L * 1024 * 1024
    }
}
