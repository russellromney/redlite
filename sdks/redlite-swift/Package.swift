// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "Redlite",
    platforms: [
        .iOS(.v13),
        .macOS(.v10_15),
        .tvOS(.v13),
        .watchOS(.v6)
    ],
    products: [
        .library(name: "Redlite", targets: ["Redlite"])
    ],
    targets: [
        // C FFI module - wraps libredlite_ffi
        .target(
            name: "CRedlite",
            path: "Sources/CRedlite",
            publicHeadersPath: "include",
            linkerSettings: [
                .unsafeFlags(["-L../../crates/redlite-ffi/target/release"]),
                .linkedLibrary("redlite_ffi")
            ]
        ),

        // Main Swift library
        .target(
            name: "Redlite",
            dependencies: ["CRedlite"],
            path: "Sources/Redlite",
            swiftSettings: [
                .enableExperimentalFeature("StrictConcurrency")
            ]
        ),

        // Tests
        .testTarget(
            name: "RedliteTests",
            dependencies: ["Redlite"],
            path: "Tests/RedliteTests"
        )
    ]
)
