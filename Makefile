PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXT_NAME=moraine
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# The distribution tooling exports the checkout hash as DUCKDB_VERSION.
ifneq ($(DUCKDB_TAG),)
EXT_FLAGS += -DDUCKDB_EXPLICIT_VERSION=$(DUCKDB_TAG)
endif

include extension-ci-tools/makefiles/duckdb_extension.Makefile
