#pragma once

namespace moraine_duckdb {

// Registers the browser's synchronous request function as the process's
// HTTP transport. Compiled for Emscripten only; the S3 client sends every
// request through it.
void RegisterMoraineHttpTransport();

} // namespace moraine_duckdb
