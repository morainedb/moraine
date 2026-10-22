PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXT_NAME=moraine
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# A published build compiles DuckDB from the checkout the distribution
# tooling makes. The prebuilt DuckDB it would otherwise link comes from a
# staging bucket keyed by commit, which labels a release commit with the
# pre-release build it was cut from: that archive, and any extension
# linking it, carries a version string the release does not.
DUCKDB_PREBUILT_LIBRARY =
DUCKDB_PREBUILT_EXTENSIONS =

include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Use the release configuration without compiling DuckDB or the Rust core.
.PHONY: configure-cpp
configure-cpp: ${EXTENSION_CONFIG_STEP}
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_RELEASE_FLAGS) $(VCPKG_MANIFEST_FLAGS) -DCMAKE_BUILD_TYPE=Release -S $(DUCKDB_SRCDIR) -B build/release
