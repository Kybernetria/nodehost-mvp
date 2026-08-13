package org.nodehost.shell

import android.content.Context
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import java.io.File
import java.net.ServerSocket
import java.net.Socket
import java.security.MessageDigest
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.json.JSONArray
import org.json.JSONObject
import org.junit.After
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.nodehost.api.AcceptedOperationDispatcher
import org.nodehost.api.ApplyVmRequest
import org.nodehost.api.HostApiController
import org.nodehost.api.HostCapability
import org.nodehost.api.HostDiagnostics
import org.nodehost.api.HostImage
import org.nodehost.api.HostMutationUseCases
import org.nodehost.api.HostProfile
import org.nodehost.api.HostResourceQueries
import org.nodehost.api.HostStatus
import org.nodehost.api.HostVm
import org.nodehost.api.ImageImportRequest
import org.nodehost.api.RecoverySshGateway
import org.nodehost.api.RecoverySshSession
import org.nodehost.core.ApplyRuntimeUseCase
import org.nodehost.core.Clock
import org.nodehost.core.EnrollmentAcceptance
import org.nodehost.core.EnrollmentRepository
import org.nodehost.core.HostMesh
import org.nodehost.core.HostMeshConfiguration
import org.nodehost.core.HostMeshStatus
import org.nodehost.core.OperationIdFactory
import org.nodehost.model.DesiredRuntimeState
import org.nodehost.model.NodeEnrollment
import org.nodehost.model.OperationId
import org.nodehost.model.OperationRecord
import org.nodehost.model.OperationState
import org.nodehost.model.RuntimeId
import org.nodehost.model.RuntimeSpec
import org.nodehost.model.SensitiveValue
import org.nodehost.model.VmProfileId
import org.nodehost.qemu.QemuExit
import org.nodehost.qemu.QemuLaunchPlan
import org.nodehost.store.NodeHostDatabase
import org.nodehost.store.RoomOperationRepository
import org.nodehost.testsupport.FakeRuntimeBackend
import org.robolectric.RobolectricTestRunner

@RunWith(RobolectricTestRunner::class)
class VerticalIntegrationTest {
    private lateinit var database: NodeHostDatabase
    private lateinit var operations: RoomOperationRepository
    private lateinit var scope: CoroutineScope

    @Before fun setUp() {
        database = Room.inMemoryDatabaseBuilder(
            ApplicationProvider.getApplicationContext<Context>(), NodeHostDatabase::class.java,
        ).build()
        operations = RoomOperationRepository(database, object : Clock { override fun epochMillis() = 1_000L })
        scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    }

    @After fun tearDown() { scope.cancel(); database.close() }

    @Test fun enrollmentToUbuntuRuntimeAndSeparateGuestBootstrapIsEndToEnd() = runBlocking {
        val runtime = FakeRuntimeBackend()
        var completed = CompletableDeferred<Unit>()
        val actor = ReconciliationActor(scope, operations, runtime, events = {
            if (it is ReconciliationEvent.Completed) completed.complete(Unit)
        })
        val mesh = RecordingMesh()
        val bootstrap = RecordingBootstrapStore()
        val ids = AtomicInteger()
        val installer = EnrollmentInstaller(
            InMemoryEnrollmentRepository(), operations, mesh, bootstrap,
            wakeReconciler = { actor.wake(WakeReason.DESIRED_STATE_CHANGED) },
            clock = object : Clock { override fun epochMillis() = 1_000L },
            operationIds = OperationIdFactory { OperationId("op-${ids.incrementAndGet().toString().padStart(3, '0')}") },
            materializer = GuestBootstrapMaterializer(
                tokenFactory = { "b".repeat(43) },
            ),
        )
        val raw = enrollmentJson()
        val installed = installer.install(
            raw,
            guestBootstrapSecretJson(),
            "enrollment-key-0001",
            "a".repeat(64),
        )
        withTimeout(5_000) { completed.await() }
        completed = CompletableDeferred()
        val controller = HostApiController(
            installed.controllerAuthenticator,
            EmptyQueries,
            UnsupportedMutations,
            ApplyRuntimeUseCase(operations, OperationIdFactory { OperationId("op-controller-apply") }),
            LoopbackRecoverySshGateway(org.nodehost.qemu.RecoverySshHostPort(19922)),
            AcceptedOperationDispatcher {
                check(actor.wake(WakeReason.DESIRED_STATE_CHANGED)) { "reconciliation actor is unavailable" }
            },
        )
        assertNotNull(controller.authorize("Bearer controller-capability-000001", "PUT", "/v1/vms/default"))
        controller.applyVm(
            ApplyVmRequest("default", 2, "running", "ubuntu-2404-arm64-uefi", 1024, 2, 8, true),
            "controller-apply-0001", "controller-generation-2".toByteArray(),
        )
        // Production dispatch wakes the actor after durable acceptance; callers never wake it manually.
        withTimeout(5_000) { completed.await() }

        assertEquals("host-auth-key-00000001", mesh.configuration?.oneUseAuthKey?.value)
        assertTrue(mesh.started)
        assertEquals("ubuntu-2404-arm64-uefi", operations.loadDesiredRuntime(RuntimeId.DEFAULT)?.profileId?.value)
        assertTrue(runtime.executed.any { it.second.id == "qemu.start_process" })
        assertEquals(1, runtime.executed.count { it.second.id == "qemu.start_process" })
        assertEquals("controller-1", installed.controllerAuthenticator.authorize("Bearer controller-capability-000001", "PUT", "/v1/vms/default")?.id)
        assertNull(installed.controllerAuthenticator.authorize("Bearer wrong-controller-capability", "PUT", "/v1/vms/default"))

        val materialized = checkNotNull(bootstrap.materialized)
        assertTrue(materialized.profile.userData.contains("PasswordAuthentication no"))
        assertTrue(materialized.profile.userData.contains("lock_passwd: true"))
        assertFalse(materialized.profile.userData.contains("password:"))
        val redemptions = (1..20).map { async(Dispatchers.Default) { materialized.secret.redeem(materialized.profile.token) } }.awaitAll()
        assertEquals(1, redemptions.count { it != null })
        val secret = checkNotNull(redemptions.single { it != null })
        val secretJson = JSONObject(String(secret))
        assertEquals("guest-auth-key-distinct-0001", secretJson.getJSONObject("mesh").getString("oneUseAuthKey"))
        assertEquals("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITestKey nodehost-test", secretJson.getJSONObject("ssh").getJSONArray("emergencyAuthorizedKeys").getString(0))
        assertNull(materialized.secret.redeem(materialized.profile.token))
        val replayedEnrollment = installer.install(raw, guestBootstrapSecretJson(), "enrollment-key-0001", "a".repeat(64))
        assertNull(replayedEnrollment.bootstrapProfile)
        assertSame(materialized, bootstrap.materialized)
        assertNull(materialized.secret.redeem(materialized.profile.token))

        // Controller transport disappearing is only absence of another wake; durable purpose remains RUNNING.
        assertEquals("RUNNING", operations.loadDesiredRuntime(RuntimeId.DEFAULT)?.desiredState?.name)
        actor.close()
    }

    @Test fun productionBackendGracefullyReplacesGenerationAndAppliesK3sAllocation() = runBlocking {
        val context = ApplicationProvider.getApplicationContext<Context>()
        context.applicationInfo.nativeLibraryDir = File(context.filesDir, "native-libs").apply { mkdirs() }.path
        installRuntimeArtifacts(context)
        File(context.filesDir, "vms").deleteRecursively()
        val qemu = ReplacingQemuControl(File(context.filesDir, "vms/default"))
        val bootProfiles = CopyOnWriteArrayList<VmProfileId>()
        val backend = AndroidQemuRuntimeBackend(
            context,
            desiredRuntime = { operations.loadDesiredRuntime(RuntimeId.DEFAULT) },
            beginBootToken = { profile -> bootProfiles += profile; "v".repeat(43) },
            recoveryPort = org.nodehost.qemu.RecoverySshHostPort(19922),
            qemu = qemu,
            artifactTrust = fixtureTrust(),
            gracefulStopMillis = 1_000,
        )
        val actor = ReconciliationActor(scope, operations, backend)
        backend.attachLifecycle(scope) { actor.wake(WakeReason.RUNTIME_EVENT) }
        val ids = ArrayDeque(listOf("op-production-g1", "op-production-g2"))
        val apply = ApplyRuntimeUseCase(operations, OperationIdFactory { OperationId(ids.removeFirst()) })
        val generation1 = RuntimeSpec(
            generation = 1, desiredState = DesiredRuntimeState.RUNNING,
            profileId = VmProfileId("ubuntu-2404-arm64-uefi"), memoryMiB = 1024,
            vcpus = 2, dataDiskGiB = 8,
        )
        val first = apply.apply(generation1, "production-gen-0001", "generation-1".toByteArray())
        actor.wake(WakeReason.DESIRED_STATE_CHANGED)
        withTimeout(5_000) { while (operations.load(first.id)?.state != OperationState.SUCCEEDED) kotlinx.coroutines.yield() }
        assertEquals(1L, (backend.observe(RuntimeId.DEFAULT) as org.nodehost.model.RuntimeObservation.Running).appliedGeneration)

        val generation2 = generation1.copy(
            generation = 2, profileId = VmProfileId("k3s-worker-lab"),
            memoryMiB = 2048, vcpus = 4, dataDiskGiB = 9,
        )
        val second = apply.apply(generation2, "production-gen-0002", "generation-2-k3s".toByteArray())
        actor.wake(WakeReason.DESIRED_STATE_CHANGED)
        withTimeout(5_000) { while (operations.load(second.id)?.state != OperationState.SUCCEEDED) kotlinx.coroutines.yield() }

        assertEquals(1, qemu.gracefulShutdowns)
        assertEquals(0, qemu.forceStops)
        assertEquals(1, qemu.maximumConcurrentProcesses)
        assertEquals(listOf(1L, 2L), qemu.startedRuntimes.map(RuntimeSpec::generation))
        assertEquals(generation2, qemu.startedRuntimes.last())
        assertEquals(listOf("ubuntu-2404-arm64-uefi", "k3s-worker-lab"), bootProfiles.map(VmProfileId::value))
        val running = backend.observe(RuntimeId.DEFAULT) as org.nodehost.model.RuntimeObservation.Running
        assertEquals(2L, running.appliedGeneration)
        assertTrue(running.guestReady)
        actor.close()
    }

    @Test fun restartConvergesAfterEveryEnrollmentBoundaryWithoutDuplicateAuthorityOrDesiredStateLoss() = runBlocking {
        EnrollmentPhase.entries.forEachIndexed { index, failedPhase ->
            val localDatabase = Room.inMemoryDatabaseBuilder(
                ApplicationProvider.getApplicationContext<Context>(), NodeHostDatabase::class.java,
            ).build()
            try {
                val localOperations = RoomOperationRepository(localDatabase, object : Clock { override fun epochMillis() = 1_000L })
                val repository = InMemoryEnrollmentRepository()
                val bootstrap = RecordingBootstrapStore()
                val mesh = RecordingMesh()
                val phases = RecordingPhaseStore()
                var injected = false
                fun installer() = EnrollmentInstaller(
                    repository, localOperations, mesh, bootstrap, {},
                    object : Clock { override fun epochMillis() = 1_000L },
                    phaseStore = phases,
                    operationIds = OperationIdFactory { OperationId("op-phase-${index.toString().padStart(3, '0')}") },
                    materializer = GuestBootstrapMaterializer { "p".repeat(43) },
                    boundaryHook = EnrollmentBoundaryHook { phase ->
                        if (!injected && phase == failedPhase) { injected = true; error("injected after $phase") }
                    },
                )
                val firstInstaller = installer()
                if (failedPhase == EnrollmentPhase.BOOTSTRAP_COMMITTED_API_READY) {
                    firstInstaller.install(enrollmentJson(), guestBootstrapSecretJson(), "phase-recovery-${index.toString().padStart(4, '0')}", "a".repeat(64))
                    assertTrue(runCatching { firstInstaller.markBootstrapCommittedApiReady() }.isFailure)
                    assertNotNull(installer().recoverOnStartup())
                    installer().markBootstrapCommittedApiReady()
                } else {
                    assertTrue(runCatching {
                        firstInstaller.install(enrollmentJson(), guestBootstrapSecretJson(), "phase-recovery-${index.toString().padStart(4, '0')}", "a".repeat(64))
                    }.isFailure)
                    // A new installer instance represents process restart and must finish from encrypted staging.
                    assertNotNull(installer().recoverOnStartup())
                }
                assertEquals(1, repository.acceptedCount)
                assertEquals("stopped", localOperations.loadDesiredRuntime(RuntimeId.DEFAULT)?.desiredState?.name?.lowercase())
                assertTrue(mesh.started)
                assertEquals(1, mesh.configureCalls)
                assertTrue(bootstrap.committed)
                assertEquals(
                    if (failedPhase == EnrollmentPhase.BOOTSTRAP_COMMITTED_API_READY) EnrollmentPhase.BOOTSTRAP_COMMITTED_API_READY
                    else EnrollmentPhase.HOST_MESH_ENROLLED_KEY_ERASED,
                    phases.state?.phase,
                )
            } finally { localDatabase.close() }
        }
    }

    @Test fun corruptUnapprovedAndExpiredPersistedStagesFailClosedAndClear() = runBlocking {
        val cases = listOf(
            EnrollmentRecoveryClassification.CORRUPT_STAGE to { state: EnrollmentRecoveryState ->
                state.copy(requestDigest = "0".repeat(64))
            },
            EnrollmentRecoveryClassification.UNAPPROVED_STAGE to { state: EnrollmentRecoveryState ->
                state.copy(approvedIssuerSpkiSha256 = "b".repeat(64))
            },
            EnrollmentRecoveryClassification.EXPIRED_STAGE to { state: EnrollmentRecoveryState ->
                val expired = JSONObject(state.rawEnrollment.toString(Charsets.UTF_8))
                expired.getJSONObject("metadata").put("expiresAt", "1970-01-01T00:00:01Z")
                state.copy(rawEnrollment = expired.toString().toByteArray())
            },
        )
        cases.forEachIndexed { index, (expected, corrupt) ->
            val repository = InMemoryEnrollmentRepository()
            val bootstrap = RecordingBootstrapStore()
            val phases = RecordingPhaseStore()
            var interrupted = false
            fun installer(hook: Boolean) = EnrollmentInstaller(
                repository, operations, RecordingMesh(), bootstrap, {},
                object : Clock { override fun epochMillis() = 1_000L },
                phaseStore = phases,
                operationIds = OperationIdFactory { OperationId("op-corrupt-${index.toString().padStart(3, '0')}") },
                materializer = GuestBootstrapMaterializer { "s".repeat(43) },
                boundaryHook = EnrollmentBoundaryHook { phase ->
                    if (hook && !interrupted && phase == EnrollmentPhase.VALIDATED_STAGED) {
                        interrupted = true
                        error("process stopped after staging")
                    }
                },
            )
            assertTrue(runCatching {
                installer(true).install(enrollmentJson(), guestBootstrapSecretJson(), "corrupt-stage-${index.toString().padStart(4, '0')}", "a".repeat(64))
            }.isFailure)
            phases.state = corrupt(checkNotNull(phases.state))

            val failure = runCatching { installer(false).recoverOnStartup() }.exceptionOrNull()
            assertTrue(failure is EnrollmentRecoveryException)
            assertEquals(expected, (failure as EnrollmentRecoveryException).classification)
            assertNull(repository.installed)
            assertNull(phases.state)
            assertNull(bootstrap.materialized)
        }
    }

    @Test fun enrollmentDesiredStateRejectsNonLowercaseSchemaValue() {
        val enrollment = JSONObject(enrollmentJson().toString(Charsets.UTF_8))
        enrollment.getJSONObject("initialRuntime").put("desiredState", "STOPPED")
        assertTrue(runCatching { EnrollmentJson.parse(enrollment.toString().toByteArray()) }.isFailure)
    }

    @Test fun recoveryPortAllocationRetriesPreBoundCollision() {
        val occupied = ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())
        try {
            val free = ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress()).use { it.localPort }
            val candidates = ArrayDeque(listOf(occupied.localPort, free))
            assertEquals(free, RecoverySshPortAllocator(candidate = { candidates.removeFirst() }, attempts = 2).allocate().value)
        } finally { occupied.close() }
    }

    @Test fun releaseMaterializationRejectsHttpMeshWhileDebugLabPolicyAllowsIt() {
        val enrollmentObject = JSONObject(enrollmentJson().toString(Charsets.UTF_8))
        enrollmentObject.getJSONObject("hostMesh").put("controlUrl", "http://mesh.lab.test")
        val guestObject = JSONObject(guestBootstrapSecretJson().toString(Charsets.UTF_8))
        guestObject.getJSONObject("mesh").put("controlUrl", "http://mesh.lab.test")
        val enrollment = EnrollmentJson.parse(enrollmentObject.toString().toByteArray())
        val guest = GuestBootstrapSecretJson.parse(guestObject.toString().toByteArray())
        assertTrue(runCatching {
            GuestBootstrapMaterializer(allowInsecureMeshUrls = false, tokenFactory = { "r".repeat(43) }).validate(enrollment, guest)
        }.isFailure)
        GuestBootstrapMaterializer(allowInsecureMeshUrls = true, tokenFactory = { "d".repeat(43) }).validate(enrollment, guest)
    }

    @Test fun persistedAuthorityCanEraseOneTimeCredentialsWithoutLosingRestartAuthority() {
        val enrollment = EnrollmentJson.parse(enrollmentJson())
        val operation = OperationRecord(
            OperationId("op-erasure-test"), "erasure-key-0001", "d".repeat(64), null, null, OperationState.ACCEPTED, null,
        )
        val stored = EnrollmentJson.encodeStored(enrollment, operation, includeOneTimeCredentials = false)
        assertFalse(stored.contains("controller-enrollment-0001"))
        assertFalse(stored.contains("host-auth-key-00000001"))
        val restored = EnrollmentJson.decodeStored(stored).first
        assertEquals(enrollment.hostAccess, restored.hostAccess)
        assertEquals(enrollment.artifacts, restored.artifacts)
    }

    @Test fun mismatchedBoundDocumentCannotCommitAndCorrectedRetrySucceeds() = runBlocking {
        val repository = InMemoryEnrollmentRepository()
        val installer = EnrollmentInstaller(
            repository, operations, RecordingMesh(), RecordingBootstrapStore(), {},
            object : Clock { override fun epochMillis() = 1_000L },
            operationIds = OperationIdFactory { OperationId("op-binding-test") },
            materializer = GuestBootstrapMaterializer { "c".repeat(43) },
        )
        val mismatched = JSONObject(guestBootstrapSecretJson().toString(Charsets.UTF_8))
            .getJSONObject("binding").put("enrollmentId", "other-enrollment")
        val badDocument = JSONObject(guestBootstrapSecretJson().toString(Charsets.UTF_8))
            .put("binding", mismatched).toString().toByteArray()
        assertTrue(runCatching { installer.install(enrollmentJson(), badDocument, "binding-key-0001", "a".repeat(64)) }.isFailure)
        assertNull(repository.installed)
        assertTrue(runCatching { installer.install(enrollmentJson(), guestBootstrapSecretJson(), "binding-key-0001", "b".repeat(64)) }.isFailure)
        assertNull(repository.installed)
        assertNotNull(installer.install(enrollmentJson(), guestBootstrapSecretJson(), "binding-key-0001", "a".repeat(64)))
        assertNotNull(repository.installed)
    }

    @Test fun recoverySshWorksIndependentlyOfGuestMeshAndEnforcesSingleSession() = runBlocking {
        val server = ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())
        val echoed = byteArrayOf(0x53, 0x53, 0x48)
        val serverThread = Thread {
            server.accept().use { socket ->
                val input = ByteArray(3)
                socket.getInputStream().read(input)
                socket.getOutputStream().write(echoed)
            }
        }.apply { start() }
        val gateway = LoopbackRecoverySshGateway(org.nodehost.qemu.RecoverySshHostPort(server.localPort))
        val principal = org.nodehost.core.ControllerPrincipal("controller-1", setOf("admin"))
        val session = gateway.open(RuntimeId.DEFAULT, principal)
        val duplicate = runCatching { gateway.open(RuntimeId.DEFAULT, principal) }
        assertTrue(duplicate.isFailure)
        session.write(byteArrayOf(1, 2, 3))
        assertArrayEquals(echoed, session.read(3))
        session.close()
        serverThread.join(2_000)
        server.close()
    }

    private fun guestBootstrapSecretJson(): ByteArray = JSONObject()
        .put("apiVersion", "nodehost.example/v1alpha1").put("kind", "GuestBootstrapSecret")
        .put("binding", JSONObject().put("enrollmentId", "enroll-0001").put("issuerSpkiSha256", "a".repeat(64)))
        .put("mesh", JSONObject().put("controlUrl", "https://mesh.example.test").put("oneUseAuthKey", "guest-auth-key-distinct-0001").put("hostname", "node-01-guest"))
        .put("ssh", JSONObject().put("user", "nodeadmin").put("emergencyAuthorizedKeys", JSONArray(listOf("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITestKey nodehost-test"))))
        .put("callback", JSONObject().put("readyUrl", "http://10.0.2.2:8080/v1/bootstrap/ready").put("capability", "guest-ready-capability-0001"))
        .toString().toByteArray()

    private fun enrollmentJson(): ByteArray = JSONObject()
        .put("apiVersion", "nodehost.example/v1alpha1").put("kind", "NodeEnrollment")
        .put("metadata", JSONObject().put("enrollmentId", "enroll-0001").put("nodeName", "node-01").put("expiresAt", "2099-01-01T00:00:00Z"))
        .put("controller", JSONObject().put("endpoint", "https://controller.example.test").put("spkiSha256", "a".repeat(64)).put("oneTimeEnrollmentToken", "controller-enrollment-0001"))
        .put("hostMesh", JSONObject().put("controlUrl", "https://mesh.example.test").put("oneUseAuthKey", "host-auth-key-00000001").put("hostname", "node-01").put("expectedTags", JSONArray(listOf("tag:nodehost"))))
        .put("hostAccess", JSONObject().put("controllerCapability", "controller-capability-000001").put("allowedControllerId", "controller-1"))
        .put("guestAccess", JSONObject().put("sshUser", "nodeadmin").put("emergencyAuthorizedKeys", JSONArray(listOf("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITestKey nodehost-test"))))
        .put("artifacts", JSONObject().put("repositoryUrl", "https://artifacts.example.test").put("profileIds", JSONArray(listOf("ubuntu-2404-arm64-uefi"))))
        .put("initialRuntime", JSONObject().put("profileId", "ubuntu-2404-arm64-uefi").put("desiredState", "stopped").put("memoryMiB", 1024).put("vcpus", 2).put("dataDiskGiB", 8))
        .toString().toByteArray()

    private object EmptyQueries : HostResourceQueries {
        override suspend fun status() = HostStatus("device-test", "running", "running")
        override suspend fun capabilities() = emptyList<HostCapability>()
        override suspend fun profiles() = emptyList<HostProfile>()
        override suspend fun images() = emptyList<HostImage>()
        override suspend fun vms() = emptyList<HostVm>()
        override suspend fun vm(id: RuntimeId): HostVm? = null
        override suspend fun operations() = emptyList<OperationRecord>()
        override suspend fun operation(id: String): OperationRecord? = null
        override suspend fun diagnostics() = HostDiagnostics(1, emptyMap())
    }

    private object UnsupportedMutations : HostMutationUseCases {
        override suspend fun importImage(request: ImageImportRequest, idempotencyKey: String, canonicalRequest: ByteArray): OperationRecord = error("not used")
        override suspend fun removeVm(id: RuntimeId, idempotencyKey: String, canonicalRequest: ByteArray): OperationRecord = error("not used")
        override suspend fun cancelOperation(id: String, idempotencyKey: String, canonicalRequest: ByteArray): OperationRecord = error("not used")
        override suspend fun revokeController(id: String, idempotencyKey: String, canonicalRequest: ByteArray) = error("not used")
    }

    private fun installRuntimeArtifacts(context: Context) {
        val root = File(context.filesDir, "nodehost-artifacts").apply { deleteRecursively(); mkdirs() }
        listOf(
            "podroid-kernel", "podroid-initramfs", "podroid-alpine-squashfs",
            "ubuntu-2404-arm64-cloud", "aavmf-code", "aavmf-vars",
        ).forEach { id ->
            val bytes = "vertical-fixture-$id".toByteArray()
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
            val bytes = "vertical-fixture-$id".toByteArray()
            TrustedArtifact(
                MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) },
                bytes.size.toLong(),
            )
        },
    )

    private class ReplacingQemuControl(private val instance: File) : QemuProcessControl {
        private var activeExit: CompletableDeferred<QemuExit>? = null
        val startedRuntimes = CopyOnWriteArrayList<RuntimeSpec>()
        var gracefulShutdowns = 0
        var forceStops = 0
        var maximumConcurrentProcesses = 0
        private var concurrentProcesses = 0

        override suspend fun start(plan: QemuLaunchPlan, runtime: RuntimeSpec): ManagedQemuProcess {
            check(activeExit == null) { "a second QEMU process started concurrently" }
            val exit = CompletableDeferred<QemuExit>()
            activeExit = exit
            concurrentProcesses++
            maximumConcurrentProcesses = maxOf(maximumConcurrentProcesses, concurrentProcesses)
            startedRuntimes += runtime
            instance.mkdirs()
            File(instance, "qmp.sock").createNewFile()
            File(instance, "guest-ready").createNewFile()
            return ManagedQemuProcess(
                processId = 100L + runtime.generation,
                awaitExit = {
                    exit.await().also {
                        if (activeExit === exit) activeExit = null
                        concurrentProcesses--
                    }
                },
                requestGuestShutdown = {
                    gracefulShutdowns++
                    exit.complete(QemuExit(0, listOf("graceful")))
                },
            )
        }

        override fun forceStop() {
            forceStops++
            activeExit?.complete(QemuExit(137, emptyList()))
        }
    }

    private class RecordingBootstrapStore : GuestBootstrapStore {
        var materialized: MaterializedGuestBootstrap? = null
        var committed = false
        override suspend fun save(materialized: MaterializedGuestBootstrap) { this.materialized = materialized; committed = false }
        override suspend fun hasDurableState(): Boolean = materialized != null
        override suspend fun enrollmentId(): String? = materialized?.profile?.enrollmentId
        override suspend fun clear() { materialized = null; committed = false }
        override suspend fun commit(enrollmentId: String): Boolean {
            if (materialized?.profile?.enrollmentId != enrollmentId) return false
            committed = true
            return true
        }
    }

    private class RecordingMesh : HostMesh {
        var configuration: HostMeshConfiguration? = null
        var started = false
        var configureCalls = 0
        override suspend fun configure(configuration: HostMeshConfiguration) { this.configuration = configuration; configureCalls++ }
        override suspend fun start() { started = true }
        override suspend fun stop() { started = false }
        override suspend fun status() = HostMeshStatus(if (started) HostMeshStatus.State.RUNNING else HostMeshStatus.State.STOPPED)
        override suspend fun clearIdentity() { started = false; configuration = null }
    }

    private class RecordingPhaseStore : EnrollmentPhaseStore {
        var state: EnrollmentRecoveryState? = null
        override suspend fun load() = state
        override suspend fun save(state: EnrollmentRecoveryState) { this.state = state.copy(
            rawEnrollment = state.rawEnrollment.copyOf(),
            rawGuestBootstrapSecret = state.rawGuestBootstrapSecret.copyOf(),
        ) }
        override suspend fun clear() { state = null }
    }

    private class InMemoryEnrollmentRepository : EnrollmentRepository {
        var installed: Pair<NodeEnrollment, OperationRecord>? = null
        var acceptedCount = 0
        override suspend fun load() = installed?.first
        override suspend fun acceptEnrollment(enrollment: NodeEnrollment, operation: OperationRecord): EnrollmentAcceptance {
            if (installed != null) return EnrollmentAcceptance.Replay(installed!!.second)
            installed = enrollment to operation
            acceptedCount++
            return EnrollmentAcceptance.Accepted
        }
    }
}
