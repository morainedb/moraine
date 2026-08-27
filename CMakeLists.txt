cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME moraine)
project(${TARGET_NAME} CXX)

# Build the moraine Rust core (crates/moraine-duckdb) as a static library and
# expose it as the CMake target `moraine-duckdb`. Corrosion forwards the
# crate's native link requirements (frameworks, system libs) to consumers.
include(FetchContent)
FetchContent_Declare(
    Corrosion
    GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
    GIT_TAG v0.5.1
)
FetchContent_MakeAvailable(Corrosion)

corrosion_import_crate(
    MANIFEST_PATH ${CMAKE_CURRENT_SOURCE_DIR}/crates/moraine-duckdb/Cargo.toml
    CRATE_TYPES staticlib
)

set(CPP_DIR ${CMAKE_CURRENT_SOURCE_DIR}/crates/moraine-duckdb/cpp)
set(EXTENSION_SOURCES
    ${CPP_DIR}/moraine_extension.cpp
    ${CPP_DIR}/storage_extension.cpp
    ${CPP_DIR}/catalog.cpp
    ${CPP_DIR}/checkpoints.cpp
    ${CPP_DIR}/census.cpp
    ${CPP_DIR}/delete_located.cpp
    ${CPP_DIR}/index_functions.cpp
    ${CPP_DIR}/inline_tables.cpp
    ${CPP_DIR}/maintenance.cpp
    ${CPP_DIR}/metadata_tables.cpp
    ${CPP_DIR}/migrate.cpp
    ${CPP_DIR}/optimizer.cpp
    ${CPP_DIR}/s3_secret.cpp
    ${CPP_DIR}/scan.cpp
    ${CPP_DIR}/staged_write.cpp
    ${CPP_DIR}/transaction_manager.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} "-no-warnings" ${EXTENSION_SOURCES})

# Native libraries the Rust static library depends on (object_store, TLS,
# async runtime). Corrosion forwards most; these cover the macOS frameworks
# and resolver explicitly, mirroring DuckDB's own Rust-backed delta extension.
if(APPLE)
    set(RUST_PLATFORM_LIBS m c resolv
        "-framework CoreFoundation" "-framework SystemConfiguration" "-framework Security"
        "-framework IOKit")
elseif(UNIX)
    set(RUST_PLATFORM_LIBS m c resolv)
else()
    set(RUST_PLATFORM_LIBS "")
endif()

foreach(EXT_TARGET ${TARGET_NAME}_extension ${TARGET_NAME}_loadable_extension)
    # The shim uses std::optional; DuckDB itself pins the global standard to
    # C++11, so force C++17 per target (a target property overrides the
    # global CMAKE_CXX_STANDARD).
    set_target_properties(${EXT_TARGET} PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
    target_include_directories(${EXT_TARGET} PRIVATE ${CPP_DIR})
    # corrosion derives its target name from the cargo *lib* name
    # (underscores), not the package name, and exposes the imported static
    # library as `<lib>-static` (GLOBAL, with the cargo build dependency and
    # forwarded native libs).
    target_link_libraries(${EXT_TARGET} moraine_duckdb-static ${RUST_PLATFORM_LIBS})
endforeach()
