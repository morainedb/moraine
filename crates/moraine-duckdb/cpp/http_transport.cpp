// The browser build's HTTP transport: one synchronous XMLHttpRequest per
// request, legal in the Web Worker DuckDB-Wasm runs in and nowhere else.

#include "http_transport.hpp"

#include "catalog.hpp"
#include "moraine_abi.h"

#include <emscripten.h>
#include <cstdint>
#include <string>
#include <vector>

// Performs the request and keeps the reply for the copy-out calls below.
// Returns the HTTP status, or 0 when no response arrived. Header lines are
// "Name: value" joined by CRLF; a header the browser refuses to set (it owns
// Host, Content-Length, and User-Agent) is skipped.
EM_JS(int, moraine_http_send_sync,
      (const char *method, const char *url, const char *header_lines, const uint8_t *body, size_t body_len), {
	      var xhr = new XMLHttpRequest();
	      try {
		      xhr.open(UTF8ToString(method), UTF8ToString(url), false);
		      xhr.responseType = "arraybuffer";
		      var lines = UTF8ToString(header_lines).split("\r\n");
		      for (var i = 0; i < lines.length; i++) {
			      var at = lines[i].indexOf(": ");
			      if (at <= 0) {
				      continue;
			      }
			      try {
				      xhr.setRequestHeader(lines[i].slice(0, at), lines[i].slice(at + 2));
			      } catch (refused) {
			      }
		      }
		      xhr.send(body_len > 0 ? HEAPU8.slice(body, body + body_len) : null);
	      } catch (failed) {
		      return 0;
	      }
	      if (xhr.status == 0) {
		      return 0;
	      }
	      globalThis.__moraine_http_reply = {
		      headers : xhr.getAllResponseHeaders() || "",
		      body : xhr.response ? new Uint8Array(xhr.response) : new Uint8Array(0)
	      };
	      return xhr.status;
      });

// The byte length the kept reply's header text needs, NUL included.
EM_JS(size_t, moraine_http_reply_header_bytes, (void),
      { return lengthBytesUTF8(globalThis.__moraine_http_reply.headers) + 1; });

// The byte length of the kept reply's body.
EM_JS(size_t, moraine_http_reply_body_bytes, (void), { return globalThis.__moraine_http_reply.body.length; });

// Copies the kept reply out and forgets it.
EM_JS(void, moraine_http_take_reply, (char *headers, size_t header_capacity, uint8_t *body), {
	var reply = globalThis.__moraine_http_reply;
	stringToUTF8(reply.headers, headers, header_capacity);
	HEAPU8.set(reply.body, body);
	globalThis.__moraine_http_reply = null;
});

namespace moraine_duckdb {

namespace {

// Splits "Name: value\r\n..." into parallel name and value lists.
void ParseHeaderLines(const std::string &text, std::vector<std::string> &names, std::vector<std::string> &values) {
	size_t start = 0;
	while (start < text.size()) {
		size_t end = text.find("\r\n", start);
		if (end == std::string::npos) {
			end = text.size();
		}
		size_t separator = text.find(": ", start);
		if (separator != std::string::npos && separator < end) {
			names.push_back(text.substr(start, separator - start));
			values.push_back(text.substr(separator + 2, end - separator - 2));
		}
		start = end + 2;
	}
}

bool Send(void *, const MoraineHttpRequest *request, MoraineHttpReceiver receiver, void *output) {
	std::string header_lines;
	for (uintptr_t index = 0; index < request->header_count; index++) {
		header_lines.append(request->header_names[index])
		    .append(": ")
		    .append(request->header_values[index])
		    .append("\r\n");
	}
	int status =
	    moraine_http_send_sync(request->method, request->url, header_lines.c_str(), request->body, request->body_len);
	if (status <= 0) {
		return false;
	}

	std::string header_text(moraine_http_reply_header_bytes(), '\0');
	std::vector<uint8_t> body(moraine_http_reply_body_bytes());
	moraine_http_take_reply(&header_text[0], header_text.size(), body.data());
	header_text.resize(header_text.find('\0'));

	std::vector<std::string> names;
	std::vector<std::string> values;
	ParseHeaderLines(header_text, names, values);
	std::vector<const char *> name_pointers;
	std::vector<const char *> value_pointers;
	for (size_t index = 0; index < names.size(); index++) {
		name_pointers.push_back(names[index].c_str());
		value_pointers.push_back(values[index].c_str());
	}
	receiver(output, static_cast<uint16_t>(status), name_pointers.data(), value_pointers.data(), name_pointers.size(),
	         body.data(), body.size());
	return true;
}

} // namespace

void RegisterMoraineHttpTransport() {
	MoraineHttpTransport transport {nullptr, Send};
	MoraineError err {};
	if (moraine_register_http_transport(transport, &err) != MORAINE_OK) {
		ThrowMoraineError(err);
	}
}

} // namespace moraine_duckdb
