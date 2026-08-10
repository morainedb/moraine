if(NOT DEFINED DUCKLAKE_PATCH_SOURCE)
    message(FATAL_ERROR "DUCKLAKE_PATCH_SOURCE must name the patched DuckLake checkout")
endif()

duckdb_extension_load(ducklake
    SOURCE_DIR ${DUCKLAKE_PATCH_SOURCE}
)
