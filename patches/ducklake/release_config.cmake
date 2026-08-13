include(FetchContent)
find_package(Git REQUIRED)

file(STRINGS "${CMAKE_CURRENT_LIST_DIR}/source-pins" DUCKLAKE_SOURCE_PINS
    REGEX "^v[0-9]+\\.[0-9]+\\.[0-9]+ ")
set(DUCKLAKE_SOURCE_COMMIT "")
foreach(PIN IN LISTS DUCKLAKE_SOURCE_PINS)
    string(REPLACE " " ";" PIN_FIELDS "${PIN}")
    list(GET PIN_FIELDS 0 PIN_DUCKDB_VERSION)
    list(GET PIN_FIELDS 1 PIN_DUCKLAKE_COMMIT)
    if(PIN_DUCKDB_VERSION STREQUAL DUCKDB_VERSION)
        set(DUCKLAKE_SOURCE_COMMIT "${PIN_DUCKLAKE_COMMIT}")
    endif()
endforeach()

if(DUCKLAKE_SOURCE_COMMIT STREQUAL "")
    message(FATAL_ERROR "No patched DuckLake source pin for DuckDB ${DUCKDB_VERSION}")
endif()

FetchContent_Declare(moraine_patched_ducklake
    GIT_REPOSITORY https://github.com/duckdb/ducklake.git
    GIT_TAG ${DUCKLAKE_SOURCE_COMMIT}
    PATCH_COMMAND
        ${GIT_EXECUTABLE} apply --unidiff-zero
        ${CMAKE_CURRENT_LIST_DIR}/0001-perf-prune-DuckLake-files-by-row-id.patch
)
FetchContent_GetProperties(moraine_patched_ducklake)
if(NOT moraine_patched_ducklake_POPULATED)
    FetchContent_Populate(moraine_patched_ducklake)
endif()

duckdb_extension_load(ducklake
    SOURCE_DIR ${moraine_patched_ducklake_SOURCE_DIR}
)
