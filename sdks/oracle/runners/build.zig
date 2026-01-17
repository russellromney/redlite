const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Path to the compiled library
    const lib_path = b.path("../../../target/release/");
    const include_path = b.path("../../../crates/redlite-ffi/");
    const sdk_path = b.path("../../redlite-zig/src/redlite.zig");

    // Create the redlite SDK module
    const redlite_mod = b.createModule(.{
        .root_source_file = sdk_path,
        .target = target,
        .optimize = optimize,
    });
    redlite_mod.addIncludePath(include_path);
    redlite_mod.addLibraryPath(lib_path);
    redlite_mod.linkSystemLibrary("redlite_ffi", .{});
    redlite_mod.linkSystemLibrary("c", .{});

    // Build the zig_runner executable
    const runner_mod = b.createModule(.{
        .root_source_file = b.path("zig_runner.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "redlite", .module = redlite_mod },
        },
    });
    runner_mod.addIncludePath(include_path);
    runner_mod.addLibraryPath(lib_path);
    runner_mod.linkSystemLibrary("redlite_ffi", .{});
    runner_mod.linkSystemLibrary("c", .{});

    const exe = b.addExecutable(.{
        .name = "zig_runner",
        .root_module = runner_mod,
    });

    // On macOS, we need to set the rpath
    if (target.result.os.tag == .macos) {
        exe.addRPath(lib_path);
    }

    b.installArtifact(exe);
}
