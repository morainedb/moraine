#!/bin/bash
# The CLI flags that open the lake, sourced by load, query and check so
# the three cannot drift apart on how the catalog is attached.
#
# META_DATA_PATH lets the catalog read the data files it registers, which
# the commit path needs; it is inert for a plain scan.
. ./env.sh

lake_flags=(
    -unsigned
    -batch
    # DuckDB reshapes its output when it detects an agent (AI_AGENT in the
    # environment), which would break the timing this harness reads back.
    -no-agent
    -c "LOAD '$MORAINE_EXTENSION'"
    -c "ATTACH 'ducklake:moraine:catalog' AS lake (DATA_PATH 'data/', META_DATA_PATH 'data/')"
    -c "USE lake"
)
