cmake_minimum_required(VERSION 3.16)
project(redlite-cpp VERSION 0.1.0 LANGUAGES CXX)

# Require C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Options
option(REDLITE_BUILD_TESTS "Build tests" ON)
option(REDLITE_BUILD_EXAMPLES "Build examples" ON)

# Find the redlite native library
# First check for custom path, then system paths
set(REDLITE_LIB_PATH "" CACHE PATH "Path to libredlite_ffi library")

if(REDLITE_LIB_PATH)
    set(REDLITE_SEARCH_PATHS ${REDLITE_LIB_PATH})
else()
    set(REDLITE_SEARCH_PATHS
        ${CMAKE_SOURCE_DIR}/../../crates/redlite-ffi/target/release
        ${CMAKE_SOURCE_DIR}/../../crates/redlite-ffi/target/debug
        /usr/local/lib
        /usr/lib
    )
endif()

find_library(REDLITE_LIBRARY
    NAMES redlite_ffi libredlite_ffi
    PATHS ${REDLITE_SEARCH_PATHS}
    NO_DEFAULT_PATH
)

if(NOT REDLITE_LIBRARY)
    find_library(REDLITE_LIBRARY NAMES redlite_ffi libredlite_ffi)
endif()

if(NOT REDLITE_LIBRARY)
    message(WARNING "libredlite_ffi not found. Build the FFI first with: cd ../../crates/redlite-ffi && cargo build --release")
endif()

# Header-only library (interface library)
add_library(redlite INTERFACE)

target_include_directories(redlite INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

if(REDLITE_LIBRARY)
    target_link_libraries(redlite INTERFACE ${REDLITE_LIBRARY})
endif()

# Platform-specific linking
if(APPLE)
    target_link_libraries(redlite INTERFACE "-framework Security" "-framework CoreFoundation")
elseif(UNIX)
    target_link_libraries(redlite INTERFACE pthread dl m)
endif()

# Tests
if(REDLITE_BUILD_TESTS)
    enable_testing()

    # Download and configure Catch2
    include(FetchContent)
    FetchContent_Declare(
        Catch2
        GIT_REPOSITORY https://github.com/catchorg/Catch2.git
        GIT_TAG v3.5.0
    )
    FetchContent_MakeAvailable(Catch2)

    # Test executables
    add_executable(test_strings tests/test_strings.cpp)
    target_link_libraries(test_strings PRIVATE redlite Catch2::Catch2WithMain)
    add_test(NAME test_strings COMMAND test_strings)

    add_executable(test_keys tests/test_keys.cpp)
    target_link_libraries(test_keys PRIVATE redlite Catch2::Catch2WithMain)
    add_test(NAME test_keys COMMAND test_keys)

    add_executable(test_hashes tests/test_hashes.cpp)
    target_link_libraries(test_hashes PRIVATE redlite Catch2::Catch2WithMain)
    add_test(NAME test_hashes COMMAND test_hashes)

    add_executable(test_lists tests/test_lists.cpp)
    target_link_libraries(test_lists PRIVATE redlite Catch2::Catch2WithMain)
    add_test(NAME test_lists COMMAND test_lists)

    add_executable(test_sets tests/test_sets.cpp)
    target_link_libraries(test_sets PRIVATE redlite Catch2::Catch2WithMain)
    add_test(NAME test_sets COMMAND test_sets)

    add_executable(test_zsets tests/test_zsets.cpp)
    target_link_libraries(test_zsets PRIVATE redlite Catch2::Catch2WithMain)
    add_test(NAME test_zsets COMMAND test_zsets)
endif()

# Examples
if(REDLITE_BUILD_EXAMPLES)
    add_executable(basic_example examples/basic.cpp)
    target_link_libraries(basic_example PRIVATE redlite)
endif()

# Installation
include(GNUInstallDirs)

install(TARGETS redlite
    EXPORT redlite-targets
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(DIRECTORY include/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(EXPORT redlite-targets
    FILE redlite-config.cmake
    NAMESPACE redlite::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/redlite
)

# pkg-config
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/redlite.pc.in
    ${CMAKE_CURRENT_BINARY_DIR}/redlite.pc
    @ONLY
)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/redlite.pc
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
)
