const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Path to the compiled library
    const lib_path = b.path("../../crates/redlite-ffi/target/release/");
    const include_path = b.path("../../crates/redlite-ffi/");

    // Create the redlite module
    const redlite_mod = b.createModule(.{
        .root_source_file = b.path("src/redlite.zig"),
        .target = target,
        .optimize = optimize,
    });
    redlite_mod.addIncludePath(include_path);
    redlite_mod.addLibraryPath(lib_path);
    redlite_mod.linkSystemLibrary("redlite_ffi", .{});
    redlite_mod.linkSystemLibrary("c", .{});

    // Main library
    const lib = b.addLibrary(.{
        .name = "redlite-zig",
        .linkage = .static,
        .root_module = redlite_mod,
    });

    // On macOS, we need to set the rpath
    if (target.result.os.tag == .macos) {
        lib.addRPath(lib_path);
    }

    b.installArtifact(lib);

    // Executable example
    const example_mod = b.createModule(.{
        .root_source_file = b.path("examples/basic.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "redlite", .module = redlite_mod },
        },
    });
    example_mod.addIncludePath(include_path);
    example_mod.addLibraryPath(lib_path);
    example_mod.linkSystemLibrary("redlite_ffi", .{});
    example_mod.linkSystemLibrary("c", .{});

    const example = b.addExecutable(.{
        .name = "example",
        .root_module = example_mod,
    });

    if (target.result.os.tag == .macos) {
        example.addRPath(lib_path);
    }

    const example_step = b.step("example", "Run the example");
    const run_example = b.addRunArtifact(example);
    example_step.dependOn(&run_example.step);

    // Unit tests
    const test_mod = b.createModule(.{
        .root_source_file = b.path("tests/test_basic.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "redlite", .module = redlite_mod },
        },
    });
    test_mod.addIncludePath(include_path);
    test_mod.addLibraryPath(lib_path);
    test_mod.linkSystemLibrary("redlite_ffi", .{});
    test_mod.linkSystemLibrary("c", .{});

    const unit_tests = b.addTest(.{
        .root_module = test_mod,
    });

    if (target.result.os.tag == .macos) {
        unit_tests.addRPath(lib_path);
    }

    const run_unit_tests = b.addRunArtifact(unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);

    // Check step for type checking without running
    const check_step = b.step("check", "Check for compile errors");
    check_step.dependOn(&lib.step);
}