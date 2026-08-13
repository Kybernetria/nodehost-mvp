package org.nodehost.shell

import android.content.Context
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import java.io.File
import java.net.InetAddress
import java.net.URI
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import org.json.JSONArray
import org.json.JSONObject
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.nodehost.api.ImageImportRequest
import org.nodehost.core.ApplyRuntimeUseCase
import org.nodehost.core.Clock
import org.nodehost.model.OperationState
import org.nodehost.store.NodeHostDatabase
import org.nodehost.store.OperationEntity
import org.nodehost.store.RoomOperationRepository
import org.robolectric.RobolectricTestRunner

@RunWith(RobolectricTestRunner::class)
class ProductionHostApiTest {
    private lateinit var database: NodeHostDatabase
    private lateinit var operations: RoomOperationRepository
    private lateinit var context: Context

    @Before fun setUp() {
        context = ApplicationProvider.getApplicationContext()
        File(context.filesDir, "nodehost-artifacts").deleteRecursively()
        File(context.filesDir, "nodehost-imports").deleteRecursively()
        context.getSharedPreferences("nodehost_cancel_receipts_secure_v1", Context.MODE_PRIVATE).edit().clear().commit()
        database = Room.inMemoryDatabaseBuilder(context, NodeHostDatabase::class.java).build()
        operations = RoomOperationRepository(database, object : Clock { override fun epochMillis() = 1000L })
    }

    @After fun tearDown() {
        database.close()
        File(context.filesDir, "nodehost-artifacts").deleteRecursively()
        File(context.filesDir, "nodehost-imports").deleteRecursively()
    }

    @Test fun tailnetBindValueRejectsLanAndLoopback() {
        assertEquals("100.64.0.9", TailnetBindAddress("100.64.0.9").value)
        assertTrue(runCatching { TailnetBindAddress("192.168.1.2") }.isFailure)
        assertTrue(runCatching { TailnetBindAddress("127.0.0.1") }.isFailure)
    }

    @Test fun globallyRoutablePolicyRejectsPrivateCgnatDocumentationAndUlaRanges() {
        val mutations = AndroidHostMutations(
            context, database, operations, ApplyRuntimeUseCase(operations, SecureOperationIdFactory()),
            enrolledRepositoryOrigin = { URI("https://artifacts.example.test") },
        )
        listOf("127.0.0.1", "10.0.0.1", "100.64.0.1", "192.0.2.1", "192.88.99.1", "198.51.100.1", "203.0.113.1", "fc00::1", "2001:db8::1")
            .forEach { assertFalse("expected rejected address $it", mutations.isGloballyRoutable(InetAddress.getByName(it))) }
        assertTrue(mutations.isGloballyRoutable(InetAddress.getByName("8.8.8.8")))
        assertTrue(mutations.isGloballyRoutable(InetAddress.getByName("2606:4700:4700::1111")))

        val mixedResolution = AndroidHostMutations(
            context, database, operations, ApplyRuntimeUseCase(operations, SecureOperationIdFactory()),
            enrolledRepositoryOrigin = { URI("https://artifacts.example.test") },
            addressResolver = { listOf(InetAddress.getByName("8.8.8.8"), InetAddress.getByName("127.0.0.1")) },
        )
        assertTrue(runCatching { mixedResolution.resolvePublicAddresses("artifacts.example.test") }.isFailure)
    }

    @Test fun productionQueriesAdvertiseExactlyTheThreeBackedProfiles() = runBlocking {
        val queries = AndroidHostResourceQueries(context, database, operations, StoppedMesh, { 1L })
        assertEquals(
            listOf("alpine-direct-qualification", "ubuntu-2404-arm64-uefi", "k3s-worker-lab"),
            queries.profiles().map { it.id },
        )
    }

    @Test fun packagedK3sVendorDataDeploysReviewedQualifierAndReportSemantics() {
        val vendor = context.assets.open("nodehost/k3s-worker-lab-vendor-data.yaml").bufferedReader().use { it.readText() }
        assertTrue(vendor.contains("schemaVersion: 1"))
        assertTrue(vendor.contains("joinedCluster: false"))
        assertTrue(vendor.contains("tailscaleReachable"))
        assertTrue(vendor.contains("minimumStorage"))
        assertTrue(vendor.contains("mv -f \"${'$'}temporary\" \"${'$'}output_path\""))
        assertFalse(vendor.contains("checks='cgroup-v2"))
    }

    @Test fun publicGuestBootstrapArtifactIsStrictAndCarriesSeparateOneUseKey() {
        val parsed = GuestBootstrapSecretJson.parse(guestSecret())
        assertEquals("guest-one-use-key-0001", parsed.mesh.oneUseAuthKey.value)
        assertEquals("nodeadmin", parsed.sshAccess.sshUser)
        assertFalse(parsed.raw.toString(Charsets.UTF_8).contains("host-one-use"))
        val withUnknownField = JSONObject(guestSecret().toString(Charsets.UTF_8)).put("unknown", true).toString().toByteArray()
        assertTrue(runCatching { GuestBootstrapSecretJson.parse(withUnknownField) }.isFailure)
    }

    @Test fun crashArtifactsAreRemovedWithoutDeletingPublishedPayload() {
        val mutations = AndroidHostMutations(
            context, database, operations, ApplyRuntimeUseCase(operations, SecureOperationIdFactory()),
            enrolledRepositoryOrigin = { URI("https://artifacts.example.test") },
        )
        val root = File(context.filesDir, "nodehost-artifacts").apply { deleteRecursively(); mkdirs() }
        val digest = "a".repeat(64)
        val published = File(root, "versions/image/$digest/payload").apply { parentFile!!.mkdirs(); writeText("published") }
        File(root, "image.manifest.json").writeText(JSONObject().put("version", 1).put("sha256", digest).put("sizeBytes", published.length()).put("relativePath", "versions/image/$digest/payload").toString())
        val orphan = File(root, "versions/orphan/${"b".repeat(64)}/payload").apply { parentFile!!.mkdirs(); writeText("orphan") }
        val partial = File(root, ".image.1.part").apply { writeText("partial") }

        mutations.cleanupInterruptedPublications()

        assertTrue(published.isFile)
        assertFalse(orphan.exists())
        assertFalse(partial.exists())
    }

    @Test fun failedPrivateAddressImportIsJournaledAndReplayedWithoutSecondEffect() = runBlocking {
        val mutations = AndroidHostMutations(
            context, database, operations,
            ApplyRuntimeUseCase(operations, SecureOperationIdFactory()),
            enrolledRepositoryOrigin = { URI("https://127.0.0.1") },
        )
        val request = ImageImportRequest("https://127.0.0.1/ubuntu-2404-arm64-cloud", "a".repeat(64), 1)
        val canonical = "private-address-import".toByteArray()
        assertTrue(runCatching { mutations.importImage(request, "image-import-key-0001", canonical) }.isFailure)
        val journaled = database.dao().operationByKey("image-import-key-0001")
        assertNotNull(journaled)
        assertEquals(OperationState.FAILED_RETRYABLE.name, journaled!!.state)
        val replay = mutations.importImage(request, "image-import-key-0001", canonical)
        assertEquals(OperationState.FAILED_RETRYABLE, replay.state)
    }

    @Test fun deleteReplayUsesDurableReceiptAfterDesiredStateIsGone() = runBlocking {
        val apply = ApplyRuntimeUseCase(operations, SecureOperationIdFactory())
        val mutations = AndroidHostMutations(
            context, database, operations, apply,
            enrolledRepositoryOrigin = { URI("https://artifacts.example.test") },
        )
        apply.apply(
            org.nodehost.model.RuntimeSpec(
                generation = 1,
                desiredState = org.nodehost.model.DesiredRuntimeState.RUNNING,
                profileId = org.nodehost.model.VmProfileId("alpine-direct-qualification"),
                memoryMiB = 512, vcpus = 1, dataDiskGiB = 4,
            ), "delete-source-key-0001", "source".toByteArray(),
        )
        val key = "delete-replay-key-0001"
        val canonical = "remove:default".toByteArray()
        val first = mutations.removeVm(org.nodehost.model.RuntimeId.DEFAULT, key, canonical)
        database.openHelper.writableDatabase.execSQL("DELETE FROM runtime_desired")

        val replay = mutations.removeVm(org.nodehost.model.RuntimeId.DEFAULT, key, canonical)
        assertEquals(first.id, replay.id)
        assertEquals(first.state, replay.state)
    }

    @Test fun cancellationWinningAtPublicationBoundaryCannotPublishImage() = runBlocking {
        val operationId = org.nodehost.model.OperationId("op-publication-race")
        database.dao().insertOperation(OperationEntity(
            operationId.value, "publication-race-source", "d".repeat(64), null, null,
            OperationState.FETCHING.name, "image.fetch", null, 1_000L, 1_000L,
        ))
        val root = File(context.filesDir, "nodehost-artifacts").apply { mkdirs() }
        val temporary = File(root, ".race.part").apply { writeText("payload") }
        val digest = java.security.MessageDigest.getInstance("SHA-256").digest(temporary.readBytes())
            .joinToString("") { "%02x".format(it) }
        val request = ImageImportRequest("https://artifacts.example.test/race-image", digest, temporary.length())
        val mutations = AndroidHostMutations(
            context, database, operations, ApplyRuntimeUseCase(operations, SecureOperationIdFactory()),
            enrolledRepositoryOrigin = { URI("https://artifacts.example.test") },
            beforePublicationClaim = { operations.cancelOperation(it, setOf(OperationState.FETCHING)) },
        )

        assertFalse(mutations.publishVerifiedDownload(operationId, "race-image", request, temporary))
        assertEquals(OperationState.CANCELLED, operations.load(operationId)?.state)
        assertFalse(File(root, "race-image.manifest.json").exists())
        assertFalse(File(root, "versions/race-image/$digest/payload").exists())
    }

    @Test fun cancelReceiptCapacityFailsDeterministicallyWithoutEvictingIdempotencyHistory() = runBlocking {
        val operationId = org.nodehost.model.OperationId("op-cancel-capacity")
        database.dao().insertOperation(OperationEntity(
            operationId.value, "cancel-capacity-source", "e".repeat(64), null, null,
            OperationState.FETCHING.name, "image.fetch", null, 1_000L, 1_000L,
        ))
        val mutations = AndroidHostMutations(
            context, database, operations, ApplyRuntimeUseCase(operations, SecureOperationIdFactory()),
            enrolledRepositoryOrigin = { URI("https://artifacts.example.test") },
        )
        repeat(256) { index ->
            mutations.cancelOperation(operationId.value, "cancel-capacity-${index.toString().padStart(4, '0')}", "cancel-$index".toByteArray())
        }
        val failure = runCatching {
            mutations.cancelOperation(operationId.value, "cancel-capacity-overflow", "overflow".toByteArray())
        }.exceptionOrNull()
        assertTrue(failure?.message.orEmpty().contains("cancel receipt capacity exceeded"))
        assertEquals(OperationState.CANCELLED, mutations.cancelOperation(
            operationId.value, "cancel-capacity-0000", "cancel-0".toByteArray(),
        ).state)
    }

    @Test fun concurrentCancelReplaysExactReceiptAndRejectsMismatchedKeyReuse() = runBlocking {
        val mutations = AndroidHostMutations(
            context, database, operations, ApplyRuntimeUseCase(operations, SecureOperationIdFactory()),
            enrolledRepositoryOrigin = { URI("https://127.0.0.1") },
        )
        val request = ImageImportRequest("https://127.0.0.1/image", "a".repeat(64), 1)
        val accepted = runCatching { mutations.importImage(request, "cancel-source-key-0001", "source".toByteArray()) }
        assertTrue(accepted.isFailure)
        val target = checkNotNull(database.dao().operationByKey("cancel-source-key-0001")).id
        val calls = (1..12).map { async { mutations.cancelOperation(target, "cancel-receipt-key-0001", "cancel-request".toByteArray()) } }.awaitAll()
        assertEquals(1, calls.map { it }.distinct().size)
        assertEquals(OperationState.CANCELLED, calls.first().state)
        assertTrue(runCatching { mutations.cancelOperation(target, "cancel-receipt-key-0001", "different".toByteArray()) }.isFailure)
    }

    @Test fun controllerRevocationIsDurableAndAuthenticatorFailsClosed() = runBlocking {
        val authenticator = EnrolledControllerAuthenticator(
            org.nodehost.model.SensitiveValue("controller-capability-0001"), "controller-1",
            isRevoked = { ControllerRevocations.isRevoked(database.openHelper.readableDatabase, it) },
        )
        assertNotNull(authenticator.authorize("Bearer controller-capability-0001", "GET", "/v1/status"))
        ControllerRevocations.revoke(
            database.openHelper.writableDatabase, "controller-1", "revoke-controller-0001", "b".repeat(64), 1000L,
        )
        assertEquals(null, authenticator.authorize("Bearer controller-capability-0001", "GET", "/v1/status"))
    }

    private object StoppedMesh : org.nodehost.core.HostMesh {
        override suspend fun configure(configuration: org.nodehost.core.HostMeshConfiguration) = Unit
        override suspend fun start() = Unit
        override suspend fun stop() = Unit
        override suspend fun status() = org.nodehost.core.HostMeshStatus(org.nodehost.core.HostMeshStatus.State.STOPPED)
        override suspend fun clearIdentity() = Unit
    }

    private fun guestSecret(): ByteArray = JSONObject()
        .put("apiVersion", "nodehost.example/v1alpha1")
        .put("kind", "GuestBootstrapSecret")
        .put("binding", JSONObject().put("enrollmentId", "enroll-0001").put("issuerSpkiSha256", "a".repeat(64)))
        .put("mesh", JSONObject().put("controlUrl", "https://mesh.example.test").put("oneUseAuthKey", "guest-one-use-key-0001").put("hostname", "node-guest"))
        .put("ssh", JSONObject().put("user", "nodeadmin").put("emergencyAuthorizedKeys", JSONArray(listOf("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITestKey nodehost-test"))))
        .put("callback", JSONObject().put("readyUrl", "http://10.0.2.2:8080/v1/bootstrap/ready").put("capability", "guest-ready-capability-0001"))
        .toString().toByteArray()
}
