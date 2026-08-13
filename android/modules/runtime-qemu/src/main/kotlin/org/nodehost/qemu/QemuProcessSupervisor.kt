package org.nodehost.qemu

import android.net.LocalSocket
import android.net.LocalSocketAddress
import java.io.Closeable
import java.io.File
import java.io.IOException
import java.net.SocketTimeoutException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout

/** Public lifecycle surface. Command compilation and the QMP socket remain module-internal. */
class QemuRuntimeAdapter private constructor(
    private val consoleSocketFactory: ConsoleSocketFactory,
) {
    constructor() : this(ConsoleSocketFactory { AndroidConsoleSocket() })

    private val supervisor = QemuProcessSupervisor()

    suspend fun start(plan: QemuLaunchPlan): QemuProcessHandle = supervisor.start(QemuCommandCompiler().compile(plan.resolved))
    suspend fun awaitExit(handle: QemuProcessHandle): QemuExit = handle.exit.await()

    /** Reads the guest serial stream until the profile-declared marker is observed. */
    suspend fun awaitConsoleMarker(handle: QemuProcessHandle, marker: String, timeoutMillis: Long) {
        require(marker.isNotEmpty() && marker.length <= 128 && '\u0000' !in marker)
        require(timeoutMillis > 0)
        withTimeout(timeoutMillis) {
            val socket = consoleSocketFactory.open()
            // LocalSocket reads are blocking. Close the socket as soon as cancellation
            // occurs so a deadline does not wait for another console byte.
            val closeOnCancellation = requireNotNull(currentCoroutineContext()[Job]).invokeOnCompletion {
                runCatching { socket.close() }
            }
            try {
                withContext(Dispatchers.IO) {
                    runInterruptible { socket.connect(handle.serialSocketPath) }
                    val rolling = StringBuilder()
                    val buffer = ByteArray(CONSOLE_READ_BUFFER_BYTES)
                    while (true) {
                        val count = try {
                            runInterruptible { socket.read(buffer) }
                        } catch (failure: IOException) {
                            currentCoroutineContext().ensureActive()
                            if (failure.isConsoleReadTimeout()) continue
                            throw failure
                        }
                        if (count < 0) error("guest console closed before readiness marker")
                        if (count == 0) continue
                        rolling.append(String(buffer, 0, count, Charsets.UTF_8))
                        if (rolling.contains(marker)) return@withContext
                        if (rolling.length > MAX_CONSOLE_SCAN_CHARS) {
                            rolling.delete(0, rolling.length - MAX_CONSOLE_SCAN_CHARS)
                        }
                    }
                }
            } finally {
                closeOnCancellation.dispose()
                runCatching { socket.close() }
            }
        }
    }

    suspend fun requestGuestShutdown(handle: QemuProcessHandle) {
        QmpSession(handle.qmpSocketPath).use { qmp -> qmp.connect(); qmp.systemPowerdown() }
    }
    fun requestStop() = supervisor.terminate()
    fun forceStop() = supervisor.forceTerminate()

    internal companion object {
        fun forTesting(consoleSocketFactory: ConsoleSocketFactory) = QemuRuntimeAdapter(consoleSocketFactory)

        const val CONSOLE_READ_BUFFER_BYTES = 4 * 1024
        const val MAX_CONSOLE_SCAN_CHARS = 64 * 1024
    }
}

internal fun interface ConsoleSocketFactory {
    fun open(): ConsoleSocket
}

internal interface ConsoleSocket : Closeable {
    fun connect(path: File)
    fun read(buffer: ByteArray): Int
}

private class AndroidConsoleSocket : ConsoleSocket {
    private val socket = LocalSocket()

    override fun connect(path: File) {
        socket.connect(LocalSocketAddress(path.path, LocalSocketAddress.Namespace.FILESYSTEM))
    }

    override fun read(buffer: ByteArray): Int = socket.inputStream.read(buffer)

    override fun close() = socket.close()
}

private fun IOException.isConsoleReadTimeout(): Boolean {
    if (this is SocketTimeoutException) return true
    val text = message?.lowercase() ?: return false
    return "timeout" in text || "timed out" in text
}

class QemuProcessHandle internal constructor(
    val processId: Long?,
    internal val exit: Deferred<QemuExit>,
    internal val qmpSocketPath: File,
    internal val serialSocketPath: File,
)
data class QemuExit(val code: Int, val stderrTail: List<String>)

/** Keeps launcher spawn and wait/reap in one long-lived runnable, as required by PR_SET_PDEATHSIG. */
internal class QemuProcessSupervisor(
    private val processFactory: ProcessFactory = JvmProcessFactory,
) {
    private val lock = Any()
    private var active: Lifetime? = null

    suspend fun start(descriptor: QemuLaunchDescriptor): QemuProcessHandle {
        val lifetime = synchronized(lock) {
            check(active == null) { "QEMU already running" }
            prepare(descriptor)
            Lifetime(newExecutor()).also { active = it }
        }
        lifetime.executor.execute { runLifetime(lifetime, descriptor) }
        return try {
            val process = withTimeout(START_TIMEOUT_MS) { lifetime.started.await() }
            QemuProcessHandle(
                processId(process), lifetime.exited,
                descriptor.sockets.single { it.name == "qmp.sock" },
                descriptor.sockets.firstOrNull { it.name == "serial.sock" }
                    ?: descriptor.sockets.single { it.name == "qmp.sock" },
            )
        } catch (failure: Throwable) {
            lifetime.cancelled = true
            lifetime.process?.destroyForcibly()
            throw failure
        }
    }

    fun terminate() { synchronized(lock) { active?.process }?.destroy() }
    fun forceTerminate() { synchronized(lock) { active?.process }?.destroyForcibly() }

    private fun runLifetime(lifetime: Lifetime, descriptor: QemuLaunchDescriptor) {
        var stderrThread: Thread? = null
        try {
            val process = processFactory.start(descriptor)
            lifetime.process = process
            if (lifetime.cancelled) process.destroyForcibly()
            stderrThread = Thread({ drainStderr(process, lifetime.stderrTail) }, "nodehost-qemu-stderr").apply {
                isDaemon = true
                start()
            }
            lifetime.started.complete(process)
            val code = process.waitFor()
            stderrThread.join(STDERR_JOIN_TIMEOUT_MS)
            lifetime.exited.complete(QemuExit(code, synchronized(lifetime.stderrTail) { lifetime.stderrTail.toList() }))
        } catch (failure: Throwable) {
            lifetime.started.completeExceptionally(failure)
            lifetime.exited.completeExceptionally(failure)
        } finally {
            lifetime.process = null
            descriptor.sockets.forEach { it.delete() }
            synchronized(lock) { if (active === lifetime) active = null }
            lifetime.executor.shutdown()
        }
    }

    private fun prepare(descriptor: QemuLaunchDescriptor) {
        require(descriptor.executable.isFile) { "QEMU executable is missing" }
        require(descriptor.workingDirectory.mkdirs() || descriptor.workingDirectory.isDirectory) { "cannot create instance directory" }
        descriptor.sockets.forEach { socket ->
            if (socket.exists() && !socket.delete()) error("cannot remove stale socket ${socket.name}")
        }
    }

    private fun drainStderr(process: Process, tail: MutableList<String>) {
        process.errorStream.bufferedReader().use { reader ->
            val line = StringBuilder()
            while (true) {
                val value = reader.read()
                if (value < 0) {
                    recordStderrLine(line, tail)
                    return
                }
                if (value.toChar() == '\n') {
                    recordStderrLine(line, tail)
                    line.setLength(0)
                } else if (line.length < MAX_STDERR_LINE_CHARS) {
                    line.append(value.toChar())
                }
            }
        }
    }

    private fun recordStderrLine(line: StringBuilder, tail: MutableList<String>) {
        val text = line.toString().trimEnd('\r')
        if (text.isBlank()) return
        synchronized(tail) {
            tail += text
            if (tail.size > MAX_STDERR_LINES) tail.removeAt(0)
        }
    }

    private fun processId(process: Process): Long? = runCatching {
        (Process::class.java.getMethod("pid").invoke(process) as Number).toLong()
    }.getOrNull()

    private fun newExecutor(): ExecutorService = Executors.newSingleThreadExecutor { runnable ->
        Thread(runnable, "nodehost-qemu-spawn-reap").apply { isDaemon = true }
    }

    private class Lifetime(val executor: ExecutorService) {
        val started = CompletableDeferred<Process>()
        val exited = CompletableDeferred<QemuExit>()
        val stderrTail = mutableListOf<String>()
        @Volatile var process: Process? = null
        @Volatile var cancelled = false
    }

    internal fun interface ProcessFactory { fun start(descriptor: QemuLaunchDescriptor): Process }

    private object JvmProcessFactory : ProcessFactory {
        override fun start(descriptor: QemuLaunchDescriptor): Process {
            val launcher = descriptor.launcher?.takeIf(File::isFile)
            val argv = listOfNotNull(launcher?.path, descriptor.executable.path) + descriptor.arguments
            return ProcessBuilder(argv)
                .directory(descriptor.workingDirectory)
                .redirectOutput(File("/dev/null"))
                .apply { environment().putAll(descriptor.environment) }
                .start()
        }
    }

    private companion object {
        const val START_TIMEOUT_MS = 10_000L
        const val STDERR_JOIN_TIMEOUT_MS = 1_000L
        const val MAX_STDERR_LINES = 40
        const val MAX_STDERR_LINE_CHARS = 1_024
    }
}
