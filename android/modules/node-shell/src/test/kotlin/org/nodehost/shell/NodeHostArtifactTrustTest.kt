package org.nodehost.shell

import java.io.ByteArrayInputStream
import java.io.File
import java.security.MessageDigest
import org.junit.Assert.assertTrue
import org.junit.Test

class NodeHostArtifactTrustTest {
    @Test fun packagedBytesMustMatchImmutableAnchor() {
        val expected = "signed-apk-bytes".toByteArray()
        val trust = trustFor("kernel", expected)

        assertTrue(runCatching { trust.verifyAsset("kernel", ByteArrayInputStream(expected)) }.isSuccess)
        assertTrue(runCatching {
            trust.verifyAsset("kernel", ByteArrayInputStream("same-size-tamper".toByteArray()))
        }.isFailure)
    }

    @Test fun candidateFileTamperingFailsEvenWhenSizeIsUnchanged() {
        val expected = "trusted-file".toByteArray()
        val candidate = File.createTempFile("nodehost-trust", ".bin")
        try {
            candidate.writeBytes(ByteArray(expected.size) { 'x'.code.toByte() })
            val trust = trustFor("kernel", expected)
            assertTrue(runCatching { trust.verifyFile("kernel", candidate) }.isFailure)
        } finally {
            candidate.delete()
        }
    }

    private fun trustFor(id: String, bytes: ByteArray): NodeHostArtifactTrust {
        val digest = MessageDigest.getInstance("SHA-256").digest(bytes)
            .joinToString("") { "%02x".format(it) }
        return NodeHostArtifactTrust.fromDigests(
            mapOf(id to TrustedArtifact(digest, bytes.size.toLong())),
        )
    }
}
