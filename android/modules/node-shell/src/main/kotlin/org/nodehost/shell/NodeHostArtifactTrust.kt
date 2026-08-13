package org.nodehost.shell

import android.content.Context
import java.io.File
import java.io.InputStream
import java.security.MessageDigest
import org.json.JSONObject

/** Digest and size pinned in the signed APK for an artifact that is extracted to filesDir. */
data class TrustedArtifact(
    val sha256: String,
    val sizeBytes: Long,
) {
    init {
        require(sha256.matches(HEX_SHA256)) { "artifact trust digest is invalid" }
        require(sizeBytes in 1..MAX_ARTIFACT_BYTES) { "artifact trust size is out of bounds" }
    }

    private companion object {
        val HEX_SHA256 = Regex("[a-f0-9]{64}")
        const val MAX_ARTIFACT_BYTES = 64L * 1024 * 1024 * 1024
    }
}

/**
 * Immutable artifact trust anchors read from an APK resource selected by the signed manifest.
 * Candidate files and filesDir-side metadata are never used to create or replace these anchors.
 */
class NodeHostArtifactTrust private constructor(
    private val artifacts: Map<String, TrustedArtifact>,
) {
    fun artifact(id: String): TrustedArtifact =
        requireNotNull(artifacts[id]) { "artifact is not trusted by the APK: $id" }

    fun verifyFile(id: String, file: File): TrustedArtifact {
        val trusted = artifact(id)
        require(file.isFile && file.length() == trusted.sizeBytes) {
            "trusted artifact is missing or has an unexpected size: $id"
        }
        require(sha256(file) == trusted.sha256) { "trusted artifact digest mismatch: $id" }
        return trusted
    }

    fun verifyAsset(id: String, input: InputStream): TrustedArtifact {
        val trusted = artifact(id)
        val digest = MessageDigest.getInstance("SHA-256")
        var size = 0L
        val buffer = ByteArray(COPY_BUFFER_BYTES)
        while (true) {
            val count = input.read(buffer)
            if (count < 0) break
            size += count
            require(size <= trusted.sizeBytes) { "packaged artifact exceeds its trusted size: $id" }
            digest.update(buffer, 0, count)
        }
        require(size == trusted.sizeBytes && digestHex(digest) == trusted.sha256) {
            "packaged artifact trust verification failed: $id"
        }
        return trusted
    }

    companion object {
        const val TRUST_RESOURCE_METADATA = "org.nodehost.artifact-trust"
        private const val TRUST_SCHEMA_VERSION = 1
        private const val MAX_TRUST_RESOURCE_BYTES = 16 * 1024
        private const val COPY_BUFFER_BYTES = 1024 * 1024

        fun fromApplication(context: Context): NodeHostArtifactTrust {
            val applicationInfo = context.packageManager.getApplicationInfo(
                context.packageName,
                android.content.pm.PackageManager.GET_META_DATA,
            )
            val resourceId = applicationInfo.metaData?.getInt(TRUST_RESOURCE_METADATA, 0) ?: 0
            require(resourceId != 0) { "signed APK artifact trust resource is missing" }
            context.resources.openRawResource(resourceId).use { input ->
                require(input.available() <= MAX_TRUST_RESOURCE_BYTES) {
                    "artifact trust resource is out of bounds"
                }
                return fromJson(input.bufferedReader().use { it.readText() })
            }
        }

        internal fun fromJson(json: String): NodeHostArtifactTrust {
            require(json.toByteArray(Charsets.UTF_8).size <= MAX_TRUST_RESOURCE_BYTES) {
                "artifact trust resource is out of bounds"
            }
            val root = JSONObject(json)
            require(root.getInt("schemaVersion") == TRUST_SCHEMA_VERSION) {
                "unsupported artifact trust schema"
            }
            val source = root.getJSONObject("artifacts")
            require(source.length() in 1..32) { "artifact trust resource has invalid artifact count" }
            val parsed = buildMap {
                val names = source.keys()
                while (names.hasNext()) {
                    val id = names.next()
                    require(id.matches(Regex("[a-z0-9][a-z0-9.-]{0,127}"))) {
                        "invalid artifact trust id"
                    }
                    val entry = source.getJSONObject(id)
                    put(id, TrustedArtifact(entry.getString("sha256"), entry.getLong("sizeBytes")))
                }
            }
            return NodeHostArtifactTrust(parsed)
        }

        internal fun fromDigests(artifacts: Map<String, TrustedArtifact>): NodeHostArtifactTrust {
            require(artifacts.isNotEmpty() && artifacts.size <= 32)
            return NodeHostArtifactTrust(artifacts.toMap())
        }

        private fun sha256(file: File): String = file.inputStream().use { input ->
            val digest = MessageDigest.getInstance("SHA-256")
            val buffer = ByteArray(COPY_BUFFER_BYTES)
            while (true) {
                val count = input.read(buffer)
                if (count < 0) break
                digest.update(buffer, 0, count)
            }
            digestHex(digest)
        }

        private fun digestHex(digest: MessageDigest): String =
            digest.digest().joinToString("") { "%02x".format(it) }
    }
}
