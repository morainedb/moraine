include(FetchContent)
find_package(Git REQUIRED)

# The patched DuckLake moraine bundles: fetched at the pin for this DuckDB
# and patched here, unless `DUCKLAKE_PATCH_SOURCE` names a checkout already
# prepared and verified against the series, as `cargo xtask` supplies.
if(DEFINED DUCKLAKE_PATCH_SOURCE AND NOT DUCKLAKE_PATCH_SOURCE STREQUAL "")
    set(moraine_patched_ducklake_SOURCE_DIR "${DUCKLAKE_PATCH_SOURCE}")
else()
    # Every entry line, whatever the release is called: a prerelease carries
    # a suffix the digits of a patch release do not, and matching on those
    # digits left its pin unselectable. `check-pins` holds the file to one
    # pin per supported release.
    file(STRINGS "${CMAKE_CURRENT_LIST_DIR}/source-pins" DUCKLAKE_SOURCE_PINS
        REGEX "^v[0-9]")
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

    # DuckDB patches DuckLake itself before building it, and those patches are
    # what make the pinned commit compile against this DuckDB at all. The
    # series here targets that patched source, so theirs go on first — three-way,
    # as DuckDB applies them with `patch`, which tolerates the same drift.
    if(NOT DEFINED DUCKDB_MODULE_BASE_DIR)
        message(FATAL_ERROR "DUCKDB_MODULE_BASE_DIR is unset: this file is included by DuckDB's build")
    endif()
    file(GLOB DUCKLAKE_UPSTREAM_PATCHES
        "${DUCKDB_MODULE_BASE_DIR}/.github/patches/extensions/ducklake/*.patch")
    if(DUCKLAKE_UPSTREAM_PATCHES STREQUAL "")
        message(FATAL_ERROR "DuckDB ships no DuckLake patches; the series targets the patched source")
    endif()
    list(SORT DUCKLAKE_UPSTREAM_PATCHES)

    FetchContent_Declare(moraine_patched_ducklake
        GIT_REPOSITORY https://github.com/duckdb/ducklake.git
        GIT_TAG ${DUCKLAKE_SOURCE_COMMIT}
        PATCH_COMMAND
            ${GIT_EXECUTABLE} apply --3way ${DUCKLAKE_UPSTREAM_PATCHES}
        COMMAND
            ${GIT_EXECUTABLE} apply
            ${CMAKE_CURRENT_LIST_DIR}/0001-perf-prune-DuckLake-files-by-row-id.patch
            ${CMAKE_CURRENT_LIST_DIR}/0002-feat-expose-DuckLake-data-file-ids-to-scans.patch
            ${CMAKE_CURRENT_LIST_DIR}/0003-feat-share-the-DuckLake-inlined-table-cache.patch
            ${CMAKE_CURRENT_LIST_DIR}/0004-fix-retain-files-after-unknown-commit-outcomes.patch
            ${CMAKE_CURRENT_LIST_DIR}/0005-feat-change-DuckLake-rows-by-position.patch
            ${CMAKE_CURRENT_LIST_DIR}/0006-perf-name-the-table-a-dropped-file-belongs-to.patch
            ${CMAKE_CURRENT_LIST_DIR}/0007-fix-cancel-DuckLake-metadata-work-with-its-caller.patch
            ${CMAKE_CURRENT_LIST_DIR}/0008-fix-write-row-ids-when-merging.patch
    )
    FetchContent_GetProperties(moraine_patched_ducklake)
    if(NOT moraine_patched_ducklake_POPULATED)
        FetchContent_Populate(moraine_patched_ducklake)
    endif()

endif()

# Built as its own static extension and linked into moraine's loadable, which
# registers it from its entry point; never linked into DuckDB itself.
set(MORAINE_DUCKLAKE_SOURCE_DIR "${moraine_patched_ducklake_SOURCE_DIR}"
    CACHE INTERNAL "The patched DuckLake source moraine bundles")
# Nothing is linked into DuckDB unless `duckdb_extension_statically_link`
# names it, so not naming it is what keeps this out of the CLI binary.
duckdb_extension_load(ducklake
    SOURCE_DIR ${moraine_patched_ducklake_SOURCE_DIR}
)
