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

# A zero-context hunk can apply at the right line but on the wrong side of a
# nearby return. Refuse a source tree where the row-ID stats are unreachable.
file(READ
    "${moraine_patched_ducklake_SOURCE_DIR}/src/storage/ducklake_transaction.cpp"
    DUCKLAKE_TRANSACTION_SOURCE)
string(FIND "${DUCKLAKE_TRANSACTION_SOURCE}" "auto row_id_field = FieldIndex" ROW_ID_STATS_POSITION)
if(ROW_ID_STATS_POSITION EQUAL -1)
    message(FATAL_ERROR "The DuckLake row-ID statistics patch is missing")
endif()
string(SUBSTRING "${DUCKLAKE_TRANSACTION_SOURCE}" ${ROW_ID_STATS_POSITION} 1600 ROW_ID_STATS_TAIL)
string(FIND "${ROW_ID_STATS_TAIL}" "\treturn data_file;" DATA_FILE_RETURN_POSITION)
string(FIND "${ROW_ID_STATS_TAIL}" "\n}" BUILD_DATA_FILE_END_POSITION)
if(DATA_FILE_RETURN_POSITION EQUAL -1 OR BUILD_DATA_FILE_END_POSITION EQUAL -1
   OR DATA_FILE_RETURN_POSITION GREATER BUILD_DATA_FILE_END_POSITION)
    message(FATAL_ERROR "The DuckLake row-ID statistics patch is unreachable")
endif()

duckdb_extension_load(ducklake
    SOURCE_DIR ${moraine_patched_ducklake_SOURCE_DIR}
)
