package org.nodehost.qemu

import java.io.File
import org.nodehost.model.*

internal object QemuTestFixtures {
    private const val SHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    fun artifact(id: String) = ArtifactRef(id, SHA, 1024)

    fun runtime(profile: VmProfile, memoryMiB: Int = 1024) = RuntimeSpec(
        generation = 1,
        desiredState = DesiredRuntimeState.RUNNING,
        profileId = profile.id,
        memoryMiB = memoryMiB,
        vcpus = 2,
        dataDiskGiB = 8,
    )

    fun alpineProfile() = VmProfile(
        id = VmProfileId("alpine-direct-qualification"), version = 1,
        machine = MachineSpec(cpuModel = "max"),
        boot = BootSpec.DirectKernel(artifact("podroid-kernel"), artifact("podroid-initramfs"), "podroid-compatible-v1"),
        systemDisk = SystemDiskSpec(artifact("podroid-alpine-squashfs"), DiskFormat.SQUASHFS, WritableLayer.SEPARATE_EXT4_OVERLAY),
        dataDisk = DataDiskSpec(4, true),
        initialization = InitializationSpec(InitializationKind.LEGACY_PODROID),
        recoverySsh = RecoverySshSpec(), health = HealthSpec(HealthKind.CONSOLE_MARKER, "Ready!"),
        requirements = ProfileRequirements(512, 3),
    )

    fun ubuntuProfile() = VmProfile(
        id = VmProfileId("ubuntu-2404-arm64-uefi"), version = 1,
        machine = MachineSpec(cpuModel = "max"),
        boot = BootSpec.Uefi(artifact("aavmf-code"), artifact("aavmf-vars")),
        systemDisk = SystemDiskSpec(artifact("ubuntu-2404-arm64-cloud"), DiskFormat.QCOW2, WritableLayer.QCOW2_OVERLAY),
        dataDisk = DataDiskSpec(8, true),
        initialization = InitializationSpec(InitializationKind.NOCLOUD_NET, "guest-init/ubuntu/vendor-data.yaml", "/v1/bootstrap/{token}/"),
        recoverySsh = RecoverySshSpec(), health = HealthSpec(HealthKind.METADATA_CALLBACK),
        requirements = ProfileRequirements(768, 5),
    )

    fun allocation(token: String? = null) = QemuRuntimeAllocation(
        nativeLibraryDir = File("/native"), filesDirectory = File("/files"),
        recoverySshHostPort = RecoverySshHostPort(19922), bootstrapToken = token?.let(::BootstrapToken),
    )

    fun descriptor(root: File, executable: File = File(root, "qemu")) = QemuLaunchDescriptor(
        executable = executable,
        launcher = File(root, "launcher"),
        workingDirectory = File(root, "vms/default"),
        environment = mapOf("LD_LIBRARY_PATH" to root.path),
        arguments = listOf("-display", "none"),
        sockets = listOf(
            File(root, "vms/default/qmp.sock"),
            File(root, "vms/default/serial.sock"),
        ),
    )
}
