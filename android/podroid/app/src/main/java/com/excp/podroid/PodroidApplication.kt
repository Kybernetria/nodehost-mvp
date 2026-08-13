/*
 * Podroid - Rootless Podman for Android
 * Copyright (C) 2024-2026 Podroid contributors
 *
 * Application class — extracts QEMU, kernel, and initrd assets on first run
 * (and on app upgrade when an asset's size changes).
 */
package com.excp.podroid

import android.app.Application
import android.os.Build
import android.util.Log
import android.content.Intent
import androidx.core.content.ContextCompat
import org.nodehost.shell.NodeHostArtifactTrust
import org.nodehost.shell.NodeHostAssetReadiness
import org.nodehost.shell.NodeHostAssetReadinessCoordinator
import org.nodehost.shell.NodeSupervisorService
import dagger.hilt.android.HiltAndroidApp
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.runInterruptible
import org.lsposed.hiddenapibypass.HiddenApiBypass
import java.io.File
import java.io.FileOutputStream

@HiltAndroidApp
class PodroidApplication : Application(), NodeHostAssetReadiness {

    private val appScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    // The VM launch path reads extracted files synchronously. The coordinator starts one
    // bounded attempt on demand, shares it across service recreation, and discards failures.
    private val assetsReadiness = NodeHostAssetReadinessCoordinator(appScope) { extractAssets() }
    private val artifactTrust by lazy { NodeHostArtifactTrust.fromApplication(this) }

    override fun onCreate() {
        super.onCreate()
        exemptHiddenApi()
        // Node purpose is service-owned. Starting the UI must retain, but never redefine,
        // the durable desired VM generation.
        ContextCompat.startForegroundService(this, Intent(this, NodeSupervisorService::class.java))
        // Extraction starts when the service or legacy Podroid service first asks for
        // readiness, rather than racing either service during Application.onCreate.
    }

    /**
     * Suspends until the bundled assets (qemu/, kernel, initrd, squashfs) have
     * finished extracting to [filesDir]. The foreground service awaits this
     * before launching the VM so QEMU/AVF never read a partial or missing file.
     */
    override suspend fun awaitNodeHostAssets() = assetsReadiness.awaitNodeHostAssets()

    suspend fun awaitAssetsReady() = awaitNodeHostAssets()

    // Android 14+ hides @SystemApi reflection lookups (returning NoSuchMethod
    // even via getDeclared*). Prefixes needing exemption:
    //   - Landroid/system/virtualmachine/ — AVF framework (AvfDiagnostics + AvfEngine)
    //   - Landroid/system/virtualizationservice/ — AVF AIDL parcelables
    //     (CpuOptions, VirtualMachineRawConfig, IVirtualizationService) used by
    //     AvfReflect's explicit-vCPU-count hook (issue #29).
    //   - Ljava/net/UnixDomainSocketAddress — ConsoleFanout needs UDS.of(String)
    //     which Android marks BLOCKED for untrusted_app even though the class
    //     itself is on the bootclasspath.
    // No-op on sub-P; the exemption itself never throws.
    private fun exemptHiddenApi() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.P) return
        runCatching {
            HiddenApiBypass.addHiddenApiExemptions(
                "Landroid/system/virtualmachine/",
                "Landroid/system/virtualizationservice/",
                "Landroid/system/UnixSocketAddress",
                "Ljava/net/UnixDomainSocketAddress",
            )
        }.onFailure { Log.w(TAG, "HiddenApiBypass exemption failed", it) }
    }

    private suspend fun extractAssets() {
        // Asset extraction has a self-healing version stamp: on every install
        // or upgrade `packageInfo.lastUpdateTime` changes, so we record it in
        // `.assets_stamp` and force a re-copy on mismatch. The stamp is written
        // only after every asset has been published and validated.
        val stampFile = File(filesDir, ".assets_stamp")
        val currentStamp = packageManager.getPackageInfo(packageName, 0).lastUpdateTime.toString()
        val previousStamp = runCatching { stampFile.readText() }.getOrDefault("")
        val forceCopy = previousStamp != currentStamp
        if (forceCopy) {
            Log.i(TAG, "asset stamp drift ($previousStamp → $currentStamp) — forcing re-extract")
        }

        deleteStaleTmpFiles(filesDir)
        // Verify the bytes read from the signed APK before consulting or replacing filesDir.
        // This also prevents an equal-size stale or substituted extraction from being accepted.
        listOf("vmlinuz-virt", "initrd.img", "alpine-rootfs.squashfs").forEach(::verifyPackagedAsset)
        coroutineScope {
            listOf(
                async { copyAssetDir("qemu", filesDir, forceCopy) },
                async { copyAssetIfNeeded("vmlinuz-virt", filesDir, forceCopy) },
                async { copyAssetIfNeeded("initrd.img", filesDir, forceCopy) },
                async { copyAssetIfNeeded("alpine-rootfs.squashfs", filesDir, forceCopy) },
            ).awaitAll()
        }

        // Do not publish readiness or the stamp until all top-level artifacts pass.
        requireExtractedAsset("vmlinuz-virt")
        requireExtractedAsset("initrd.img")
        requireExtractedAsset("alpine-rootfs.squashfs")
        writeStampAtomically(stampFile, currentStamp)
    }

    /** Recursively removes leftover `<name>.tmp` files under [dir]. */
    private fun deleteStaleTmpFiles(dir: File) {
        val children = dir.listFiles() ?: return
        for (child in children) {
            if (child.isDirectory) {
                deleteStaleTmpFiles(child)
            } else if (child.name.endsWith(TMP_SUFFIX)) {
                runCatching { child.delete() }
            }
        }
    }

    /**
     * Copies an asset to destDir if missing OR if the size differs OR if the
     * install-time stamp drifted. The stamp is the key bit: `mksquashfs` is
     * deterministic, so an upgrade can ship a same-size squashfs with
     * different content (e.g. an init.d script edited) — size-only checks
     * would silently keep the stale copy and the VM boots the old rootfs.
     */
    private suspend fun copyAssetIfNeeded(assetName: String, destDir: File, forceCopy: Boolean) {
        val destFile = File(destDir, assetName)
        val trustedSize = verifyPackagedAsset(assetName)
        if (!forceCopy && destFile.isFile && destFile.length() == trustedSize) {
            requireExtractedAsset(assetName, trustedSize)
            return
        }

        destFile.parentFile?.mkdirs()
        copyAssetAtomically(assetName, destFile)
        requireExtractedAsset(assetName, trustedSize)
    }

    /**
     * Walks an asset directory tree and mirrors it under destDir.
     * Each file is copied if missing OR if its size differs OR if forceCopy
     * is true (install-stamp drift).
     */
    private suspend fun copyAssetDir(assetPath: String, destDir: File, forceCopy: Boolean) {
        currentCoroutineContext().ensureActive()
        val entries = requireNotNull(assets.list(assetPath)) { "asset directory is missing: $assetPath" }
        for (entry in entries) {
            currentCoroutineContext().ensureActive()
            val src = "$assetPath/$entry"
            val dest = File(destDir, entry)
            val subEntries = assets.list(src)
            if (subEntries != null && subEntries.isNotEmpty()) {
                check(dest.mkdirs() || dest.isDirectory) { "cannot create asset directory: $dest" }
                copyAssetDir(src, dest, forceCopy)
            } else {
                copyAssetFileIfNeeded(src, dest, forceCopy)
            }
        }
    }

    private suspend fun copyAssetFileIfNeeded(assetPath: String, destFile: File, forceCopy: Boolean) {
        val assetSize = assetSize(assetPath)
        if (!forceCopy && assetSize >= 0 && destFile.isFile && destFile.length() == assetSize) {
            requireExtractedAsset(destFile.name, assetSize, destFile)
            return
        }

        copyAssetAtomically(assetPath, destFile)
        requireExtractedAsset(destFile.name, assetSize, destFile)
    }

    private fun assetSize(assetPath: String): Long =
        try {
            assets.openFd(assetPath).use { it.length }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            -1L
        }.also {
            require(it < 0 || it <= MAX_ASSET_BYTES) { "asset exceeds extraction bound: $assetPath" }
        }

    /**
     * Streams [assetPath] to `<destFile>.tmp`, fsyncs the data to disk, then
     * atomically renames it onto [destFile]. The final canonical path therefore
     * only ever holds a fully-written file — an async reader (the VM launch)
     * never sees a half-written squashfs/kernel. Throws on any failure so the
     * caller logs it and the stale/missing file is caught by the next size-check.
     */
    private fun requireExtractedAsset(assetName: String, assetSize: Long = -1L, destFile: File = File(filesDir, assetName)) {
        require(destFile.isFile && destFile.length() > 0) { "extracted asset is missing or empty: $assetName" }
        if (isTrustedBundledAsset(assetName)) {
            artifactTrust.verifyFile(assetName, destFile)
        } else if (assetSize >= 0) {
            require(destFile.length() == assetSize) {
                "extracted asset has unexpected size: $assetName"
            }
        }
    }

    private fun verifyPackagedAsset(assetName: String): Long =
        assets.open(assetName).use { artifactTrust.verifyAsset(assetName, it).sizeBytes }

    private fun isTrustedBundledAsset(assetName: String): Boolean =
        assetName == "vmlinuz-virt" || assetName == "initrd.img" || assetName == "alpine-rootfs.squashfs"

    private suspend fun copyAssetAtomically(assetPath: String, destFile: File) {
        val tmpFile = File(destFile.parentFile, destFile.name + TMP_SUFFIX)
        try {
            assets.open(assetPath).use { input ->
                FileOutputStream(tmpFile).use { output ->
                    val buffer = ByteArray(COPY_BUFFER_BYTES)
                    var written = 0L
                    while (true) {
                        currentCoroutineContext().ensureActive()
                        val count = runInterruptible { input.read(buffer) }
                        if (count < 0) break
                        written += count
                        require(written <= MAX_ASSET_BYTES) { "asset exceeds extraction bound: $assetPath" }
                        runInterruptible { output.write(buffer, 0, count) }
                    }
                    runInterruptible { output.flush(); output.fd.sync() }
                }
            }
            currentCoroutineContext().ensureActive()
            if (!tmpFile.renameTo(destFile)) {
                throw java.io.IOException("atomic rename ${tmpFile.name} -> ${destFile.name} failed")
            }
        } catch (failure: Throwable) {
            runCatching { tmpFile.delete() }
            throw failure
        }
    }

    private fun writeStampAtomically(stampFile: File, value: String) {
        val temporary = File(stampFile.parentFile, stampFile.name + TMP_SUFFIX)
        try {
            temporary.writeText(value)
            check(temporary.renameTo(stampFile)) { "asset stamp publication failed" }
        } catch (failure: Throwable) {
            runCatching { temporary.delete() }
            throw failure
        }
    }

    companion object {
        private const val TAG = "PodroidApp"
        private const val TMP_SUFFIX = ".tmp"
        private const val COPY_BUFFER_BYTES = 1024 * 1024
        private const val MAX_ASSET_BYTES = 512L * 1024 * 1024
    }
}
