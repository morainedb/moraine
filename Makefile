PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

ifeq ($(EXTENSION_NAME),ducklake)
EXT_NAME=ducklake
EXT_CONFIG=${PROJ_DIR}patches/ducklake/release_config.cmake
# DuckLake depends on roaring. Generate the vcpkg manifest from the fetched
# extension instead of moraine's empty root manifest.
USE_MERGED_VCPKG_MANIFEST=1
else
EXT_NAME=moraine
EXT_CONFIG=${PROJ_DIR}extension_config.cmake
endif

include extension-ci-tools/makefiles/duckdb_extension.Makefile
