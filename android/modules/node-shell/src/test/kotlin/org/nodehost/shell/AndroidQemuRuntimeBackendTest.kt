package org.nodehost.shell

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import java.io.File
import java.io.RandomAccessFile
import java.security.MessageDigest
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assert.assertArrayEquals
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.nodehost.core.OperationContext
import org.nodehost.core.RuntimeStep
import org.nodehost.model.BootSpec
import org.nodehost.model.DesiredRuntimeState
import org.nodehost.model.OperationId
import org.nodehost.model.RuntimeId
import org.nodehost.model.RuntimeObservation
import org.nodehost.model.RuntimeSpec
import org.nodehost.model.VmProfileId
import org.nodehost.qemu.QemuExit
import org.nodehost.qemu.QemuLaunchPlan
import org.robolectric.RobolectricTestRunner

@RunWith(RobolectricTestRunner::class)
class AndroidQemuRuntimeBackendTest {
    private lateinit var context: Context
    private lateinit var scope: CoroutineScope

    @Before fun setUp() {
        context = ApplicationProvider.getApplicationContext()
        context.applicationInfo.nativeLibraryDir = File(context.filesDir, "native-libs").apply { mkdirs() }.path
        File(context.filesDir, "nodehost-artifacts").deleteRecursively()
        File(context.filesDir, "vms").deleteRecursively()
        File(context.filesDir, "nodehost-durable").deleteRecursively()
        scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        installArtifacts()
    }

    @After fun tearDown() {
        scope.cancel()
        File(context.filesDir, "nodehost-artifacts").deleteRecursively()
        File(context.filesDir, "vms").deleteRecursively()
        File(context.filesDir, "nodehost-durable").deleteRecursively()
        listOf("vmlinuz-virt", "initrd.img", "alpine-rootfs.squashfs").forEach {
            File(context.filesDir, it).delete()
        }
    }

    @Test fun allAdvertisedProfilesResolveToBackedTypedBootModes() {
        val backend = backend(FakeQemuControl())
        assertTrue(backend.profile(VmProfileId("alpine-direct-qualification"), true).boot is BootSpec.DirectKernel)
        assertTrue(backend.profile(VmProfileId("ubuntu-2404-arm64-uefi"), true).boot is BootSpec.Uefi)
        val k3s = backend.profile(VmProfileId("k3s-worker-lab"), true)
        assertTrue(k3s.boot is BootSpec.Uefi)
        assertEquals(VmProfileId("ubuntu-2404-arm64-uefi"), k3s.extends)
        assertTrue(k3s.requirements.qualificationChecks.contains("tailscale-reachability"))
    }

    @Test fun filesDirCandidateCannotSupplyTrustAnchor() {
        listOf("vmlinuz-virt", "initrd.img", "alpine-rootfs.squashfs").forEach {
            File(context.filesDir, it).delete()
        }
        val backend = backend(FakeQemuControl())

        assertTrue(runCatching {
            backend.profile(VmProfileId("alpine-direct-qualification"), true)
        }.isFailure)
    }

    @Test fun equalSizeFilesDirSubstitutionFailsAgainstApkTrustAnchor() {
        val candidate = File(context.filesDir, "vmlinuz-virt")
        candidate.writeBytes(ByteArray(candidate.length().toInt()) { 'x'.code.toByte() })
        val backend = backend(FakeQemuControl())

        assertTrue(runCatching {
            backend.profile(VmProfileId("alpine-direct-qualification"), true)
        }.isFailure)
    }

    @Test fun bundledAlpineArtifactsAreResolvedFromPodroidFilesDirectory() = runBlocking {
        File(context.filesDir, "nodehost-artifacts").deleteRecursively()
        listOf("podroid-kernel", "podroid-initramfs", "podroid-alpine-squashfs").forEach { id ->
            val name = when (id) {
                "podroid-kernel" -> "vmlinuz-virt"
                "podroid-initramfs" -> "initrd.img"
                else -> "alpine-rootfs.squashfs"
            }
            File(context.filesDir, name).writeBytes("fixture-$id".toByteArray())
        }
        val backend = backend(FakeQemuControl(), runtime = RuntimeSpec(
            generation = 1, desiredState = DesiredRuntimeState.RUNNING,
            profileId = VmProfileId("alpine-direct-qualification"), memoryMiB = 512,
            vcpus = 1, dataDiskGiB = 4,
        ))

        backend.execute(operationContext("prepare"), RuntimeStep.PrepareDisks)

        assertTrue(File(context.filesDir, "vms/default/artifacts/vmlinuz-virt").isFile)
        assertTrue(File(context.filesDir, "vms/default/artifacts/initrd.img").isFile)
        assertTrue(File(context.filesDir, "vms/default/artifacts/alpine-rootfs.squashfs").isFile)
    }

    @Test fun mutableSystemStateSurvivesRepeatedPreparationAfterBootstrapConsumption() = runBlocking {
        val backend = backend(FakeQemuControl())
        backend.execute(operationContext("prepare-1"), RuntimeStep.PrepareDisks)
        val instance = File(context.filesDir, "vms/default")
        val systemMutation = "mutated-system".toByteArray()
        val varsMutation = "mutated-vars".toByteArray()
        File(instance, "system.qcow2").writeBytes(systemMutation)
        File(instance, "firmware-vars.fd").writeBytes(varsMutation)

        backend.execute(operationContext("prepare-2"), RuntimeStep.PrepareDisks)

        assertArrayEquals(systemMutation, File(instance, "system.qcow2").readBytes())
        assertArrayEquals(varsMutation, File(instance, "firmware-vars.fd").readBytes())
    }

    @Test fun alpinePreservedStorageDiskMovesOutBeforeDeleteAndReattachesOnRecreate() = runBlocking {
        val runtime = RuntimeSpec(
            generation = 1, desiredState = DesiredRuntimeState.RUNNING,
            profileId = VmProfileId("alpine-direct-qualification"), memoryMiB = 512,
            vcpus = 1, dataDiskGiB = 4, preserveDataOnDelete = true,
        )
        val backend = backend(FakeQemuControl(), runtime = runtime)
        backend.execute(operationContext("prepare"), RuntimeStep.PrepareDisks)
        val data = File(context.filesDir, "vms/default/storage.img")
        RandomAccessFile(data, "rw").use { it.seek(7); it.write(byteArrayOf(42)) }

        backend.execute(operationContext("remove"), RuntimeStep.RemoveSystem)
        assertTrue(File(context.filesDir, "nodehost-durable/vms/default/storage.img").isFile)
        assertFalse(File(context.filesDir, "nodehost-durable/vms/default/data.raw").exists())
        backend.execute(operationContext("recreate"), RuntimeStep.PrepareDisks)
        RandomAccessFile(data, "r").use { it.seek(7); assertEquals(42, it.read()) }
    }

    @Test fun preservedDataMovesOutBeforeDeleteAndReattachesOnRecreate() = runBlocking {
        val runtime = testRuntime(preserveData = true)
        val backend = backend(FakeQemuControl(), runtime = runtime)
        backend.execute(operationContext("prepare"), RuntimeStep.PrepareDisks)
        val data = File(context.filesDir, "vms/default/data.raw")
        RandomAccessFile(data, "rw").use { it.seek(7); it.write(byteArrayOf(42)) }

        backend.execute(operationContext("remove"), RuntimeStep.RemoveSystem)
        assertFalse(File(context.filesDir, "vms/default").exists())
        assertTrue(File(context.filesDir, "nodehost-durable/vms/default/data.raw").isFile)
        backend.execute(operationContext("recreate"), RuntimeStep.PrepareDisks)
        RandomAccessFile(data, "r").use { it.seek(7); assertEquals(42, it.read()) }
    }

    @Test fun failedDataMoveAbortsDeletion() = runBlocking {
        val backend = backend(FakeQemuControl(), runtime = testRuntime(preserveData = true), move = { _, _ -> error("move failed") })
        backend.execute(operationContext("prepare"), RuntimeStep.PrepareDisks)
        assertTrue(runCatching { backend.execute(operationContext("remove"), RuntimeStep.RemoveSystem) }.isFailure)
        assertTrue(File(context.filesDir, "vms/default/system.qcow2").isFile)
        assertTrue(File(context.filesDir, "vms/default/data.raw").isFile)
    }

    @Test fun alpineReadinessWaitsForConsoleMarker() = runBlocking {
        val qemu = FakeQemuControl()
        val backend = backend(qemu, runtime = RuntimeSpec(
            generation = 1, desiredState = DesiredRuntimeState.RUNNING,
            profileId = VmProfileId("alpine-direct-qualification"), memoryMiB = 512,
            vcpus = 1, dataDiskGiB = 4,
        ))
        backend.execute(operationContext("prepare"), RuntimeStep.PrepareBoot)
        backend.execute(operationContext("start"), RuntimeStep.StartProcess)
        File(context.filesDir, "vms/default/qmp.sock").apply { parentFile!!.mkdirs(); createNewFile() }
        backend.execute(operationContext("qmp"), RuntimeStep.WaitForQmp)

        assertTrue(runCatching {
            backend.execute(operationContext("guest"), RuntimeStep.WaitForGuest)
        }.isFailure)
        assertFalse((backend.observe(RuntimeId.DEFAULT) as RuntimeObservation.Running).guestReady)
        qemu.consoleMarkerReady = true
        backend.execute(operationContext("guest-2"), RuntimeStep.WaitForGuest)
        assertTrue((backend.observe(RuntimeId.DEFAULT) as RuntimeObservation.Running).guestReady)
        assertEquals(listOf("Ready!", "Ready!"), qemu.consoleMarkers)
    }

    @Test fun gracefulDeadlineForceStopExitAndWakeAreObserved() = runBlocking {
        var elapsed = 0L
        var wakes = 0
        val qemu = FakeQemuControl()
        val backend = backend(qemu, elapsedRealtime = { elapsed })
        backend.attachLifecycle(scope) { wakes++ }
        backend.execute(operationContext("prepare"), RuntimeStep.PrepareBoot)
        backend.execute(operationContext("start"), RuntimeStep.StartProcess)
        assertTrue(backend.observe(RuntimeId.DEFAULT) is RuntimeObservation.Starting)

        backend.execute(operationContext("shutdown"), RuntimeStep.RequestShutdown)
        assertEquals(1, qemu.shutdownRequests)
        assertFalse((backend.observe(RuntimeId.DEFAULT) as RuntimeObservation.Stopping).gracefulDeadlineExceeded)
        elapsed = 11
        assertTrue((backend.observe(RuntimeId.DEFAULT) as RuntimeObservation.Stopping).gracefulDeadlineExceeded)

        backend.execute(operationContext("force"), RuntimeStep.ForceStop)
        withTimeout(2_000) { while (wakes == 0) kotlinx.coroutines.yield() }
        assertEquals(1, qemu.forceStops)
        assertTrue(backend.observe(RuntimeId.DEFAULT) is RuntimeObservation.Absent)
    }

    @Test fun qmpFailureKeepsOriginalDeadlineAndStillWakesForceStop() = runBlocking {
        var elapsed = 0L
        var wakes = 0
        val qemu = FakeQemuControl().also { it.shutdownFailure = IllegalStateException("QMP unavailable") }
        val backend = backend(qemu, elapsedRealtime = { elapsed })
        backend.attachLifecycle(scope) { wakes++ }
        backend.execute(operationContext("prepare"), RuntimeStep.PrepareBoot)
        backend.execute(operationContext("start"), RuntimeStep.StartProcess)

        assertTrue(runCatching {
            backend.execute(operationContext("shutdown-1"), RuntimeStep.RequestShutdown)
        }.isFailure)
        elapsed = 9
        assertTrue(runCatching {
            backend.execute(operationContext("shutdown-2"), RuntimeStep.RequestShutdown)
        }.isFailure)
        assertFalse((backend.observe(RuntimeId.DEFAULT) as RuntimeObservation.Stopping).gracefulDeadlineExceeded)
        elapsed = 11
        assertTrue((backend.observe(RuntimeId.DEFAULT) as RuntimeObservation.Stopping).gracefulDeadlineExceeded)
        withTimeout(2_000) { while (wakes == 0) kotlinx.coroutines.yield() }

        backend.execute(operationContext("force"), RuntimeStep.ForceStop)
        assertEquals(1, qemu.forceStops)
        assertTrue(backend.observe(RuntimeId.DEFAULT) is RuntimeObservation.Absent)
    }

    @Test fun runningObservationRetainsPreparedGenerationWhenDesiredChanges() = runBlocking {
        var desired = testRuntime()
        val qemu = FakeQemuControl()
        val backend = AndroidQemuRuntimeBackend(
            context,
            desiredRuntime = { desired },
            beginBootToken = { "b".repeat(43) },
            recoveryPort = org.nodehost.qemu.RecoverySshHostPort(19922),
            qemu = qemu,
            artifactTrust = fixtureTrust(),
        )
        backend.attachLifecycle(scope) {}
        backend.execute(operationContext("prepare"), RuntimeStep.PrepareBoot)
        backend.execute(operationContext("start"), RuntimeStep.StartProcess)
        desired = testRuntime().copy(generation = 2, profileId = VmProfileId("k3s-worker-lab"), memoryMiB = 2048)
        File(context.filesDir, "vms/default").mkdirs()
        File(context.filesDir, "vms/default/qmp.sock").createNewFile()
        backend.execute(operationContext("qmp"), RuntimeStep.WaitForQmp)

        val running = backend.observe(RuntimeId.DEFAULT) as RuntimeObservation.Running
        assertEquals(1L, running.appliedGeneration)
        assertEquals(1L, qemu.startedRuntimes.single().generation)

        qemu.exit.complete(QemuExit(0, emptyList()))
        withTimeout(2_000) { while (backend.observe(RuntimeId.DEFAULT) is RuntimeObservation.Running) kotlinx.coroutines.yield() }
        assertTrue(backend.observe(RuntimeId.DEFAULT) !is RuntimeObservation.Running)
    }

    @Test fun spontaneousProcessExitClearsRuntimeAndWakesReconciliation() = runBlocking {
        var wakes = 0
        val qemu = FakeQemuControl()
        val backend = backend(qemu)
        backend.attachLifecycle(scope) { wakes++ }
        backend.execute(operationContext("prepare"), RuntimeStep.PrepareBoot)
        backend.execute(operationContext("start"), RuntimeStep.StartProcess)

        qemu.exit.complete(QemuExit(17, listOf("bounded diagnostic")))
        withTimeout(2_000) { while (wakes == 0) kotlinx.coroutines.yield() }
        assertTrue(backend.observe(RuntimeId.DEFAULT) is RuntimeObservation.Absent)
    }

    private fun backend(
        qemu: FakeQemuControl,
        elapsedRealtime: () -> Long = { 0L },
        runtime: RuntimeSpec = testRuntime(),
        move: (File, File) -> Unit = { source, target -> java.nio.file.Files.move(source.toPath(), target.toPath(), java.nio.file.StandardCopyOption.ATOMIC_MOVE) },
    ) = AndroidQemuRuntimeBackend(
        context,
        desiredRuntime = { runtime },
        beginBootToken = { "b".repeat(43) },
        recoveryPort = org.nodehost.qemu.RecoverySshHostPort(19922),
        qemu = qemu,
        artifactTrust = fixtureTrust(),
        elapsedRealtimeMillis = elapsedRealtime,
        gracefulStopMillis = 10,
        forceExitMillis = 1_000,
        atomicMove = move,
    )

    private fun testRuntime(preserveData: Boolean = false) = RuntimeSpec(
        generation = 1, desiredState = DesiredRuntimeState.RUNNING,
        profileId = VmProfileId("ubuntu-2404-arm64-uefi"), memoryMiB = 1024,
        vcpus = 2, dataDiskGiB = 8, preserveDataOnDelete = preserveData,
    )

    private fun operationContext(step: String) = OperationContext(OperationId("op-qemu-test"), step, 1)

    private fun installArtifacts() {
        val root = File(context.filesDir, "nodehost-artifacts").apply { mkdirs() }
        listOf(
            "podroid-kernel", "podroid-initramfs", "podroid-alpine-squashfs",
            "ubuntu-2404-arm64-cloud", "aavmf-code", "aavmf-vars",
        ).forEach { id ->
            val bytes = "fixture-$id".toByteArray()
            File(root, id).writeBytes(bytes)
            if (id == "podroid-kernel") File(context.filesDir, "vmlinuz-virt").writeBytes(bytes)
            if (id == "podroid-initramfs") File(context.filesDir, "initrd.img").writeBytes(bytes)
            if (id == "podroid-alpine-squashfs") File(context.filesDir, "alpine-rootfs.squashfs").writeBytes(bytes)
            val digest = MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
            File(root, "$id.sha256").writeText("$digest\n")
        }
    }

    private fun fixtureTrust(): NodeHostArtifactTrust = NodeHostArtifactTrust.fromDigests(
        listOf("podroid-kernel", "podroid-initramfs", "podroid-alpine-squashfs").associateWith { id ->
            val bytes = "fixture-$id".toByteArray()
            TrustedArtifact(
                MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) },
                bytes.size.toLong(),
            )
        },
    )

    private class FakeQemuControl : QemuProcessControl {
        val exit = CompletableDeferred<QemuExit>()
        val startedRuntimes = mutableListOf<RuntimeSpec>()
        var shutdownRequests = 0
        var shutdownFailure: Throwable? = null
        var forceStops = 0
        var consoleMarkerReady = false
        val consoleMarkers = mutableListOf<String>()
        override suspend fun start(plan: QemuLaunchPlan, runtime: RuntimeSpec): ManagedQemuProcess {
            startedRuntimes += runtime
            return ManagedQemuProcess(
                42, { exit.await() }, {
                    shutdownFailure?.let { throw it }
                    shutdownRequests++
                },
                { marker, _ -> consoleMarkers += marker; check(consoleMarkerReady) { "marker not observed" } },
            )
        }
        override fun forceStop() {
            forceStops++
            exit.complete(QemuExit(137, emptyList()))
        }
    }
}
