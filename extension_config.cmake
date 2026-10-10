# Included by DuckDB's build system to discover which extensions to build.
# The patched DuckLake is built alongside and bundled into moraine's
# loadable, so one `LOAD moraine` serves both. Neither is statically linked
# into DuckDB's own CLI binary: only the loadable
# `.duckdb_extension` is built, so the Rust core never lands in it.
include(${CMAKE_CURRENT_LIST_DIR}/patches/ducklake/ducklake.cmake)

duckdb_extension_load(moraine
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/crates/moraine-duckdb/cpp
)
