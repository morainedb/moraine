# Included by DuckDB's build system to discover which extensions to build.
# The patched DuckLake is built alongside and bundled into moraine's
# loadable, so one `LOAD moraine` serves both. DONT_LINK: build only the
# loadable `.duckdb_extension`, don't statically link either (and moraine's
# Rust core) into DuckDB's own CLI binary.
include(${CMAKE_CURRENT_LIST_DIR}/patches/ducklake/ducklake.cmake)

duckdb_extension_load(moraine
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/crates/moraine-duckdb/cpp
    DONT_LINK
)
