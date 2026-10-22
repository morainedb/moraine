PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXT_NAME=moraine
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# The distribution tooling exports the checkout hash as DUCKDB_VERSION.
ifneq ($(DUCKDB_TAG),)
EXT_FLAGS += -DDUCKDB_EXPLICIT_VERSION=$(DUCKDB_TAG)
endif

include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Use the release configuration without compiling DuckDB or the Rust core.
.PHONY: configure-cpp
configure-cpp: ${EXTENSION_CONFIG_STEP}
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_RELEASE_FLAGS) $(VCPKG_MANIFEST_FLAGS) -DCMAKE_BUILD_TYPE=Release -S $(DUCKDB_SRCDIR) -B build/release

# Stamp DUCKDB_TAG onto the DuckDB checkout, overriding the included recipe
# whose `git tag` is fatal once the pin is a release the checkout already
# carries. A tag on HEAD is the stamp; one on another commit still fails.
.PHONY: set_duckdb_tag
set_duckdb_tag:
	@if [ -n "$$(git -C duckdb tag --points-at HEAD --list '$(DUCKDB_TAG)')" ]; then \
		echo "duckdb already carries $(DUCKDB_TAG)"; \
	else \
		git -C duckdb tag $(DUCKDB_TAG); \
	fi
