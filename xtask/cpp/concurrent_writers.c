// Times concurrent DuckLake transactions inside one DuckDB instance.
//
// The CLI holds a single connection, so the statement workloads can never
// make two DuckLake transactions overlap. This opens one instance, gives
// each writer its own connection, releases them together on a barrier and
// times the window. Only the ATTACH differs between catalog backends, so
// the same binary measures all of them.
//
// Usage:
//   concurrent_writers <extension-dir> <extension> <attach-sql> <writers> <commits-each>
// Prints `elapsed_seconds=<seconds>` and exits non-zero on any failure.

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "duckdb.h"

#define STATEMENT_MAX 4096

typedef struct {
    duckdb_database database;
    pthread_barrier_t *barrier;
    int writer;
    long long commits;
    char error[STATEMENT_MAX];
} writer_task;

// Runs one statement, copying any error into `error`.
static int run(duckdb_connection connection, const char *sql, char *error) {
    duckdb_result result;
    if (duckdb_query(connection, sql, &result) == DuckDBError) {
        const char *message = duckdb_result_error(&result);
        snprintf(error, STATEMENT_MAX, "`%.200s` failed: %.400s", sql, message ? message : "?");
        duckdb_destroy_result(&result);
        return 1;
    }
    duckdb_destroy_result(&result);
    return 0;
}

// Prints every warning the catalog logged during the burst.
static void report_catalog_warnings(duckdb_connection connection) {
    duckdb_result result;
    if (duckdb_query(connection,
                     "SELECT message FROM duckdb_logs WHERE type = 'moraine' "
                     "AND log_level IN ('WARN', 'ERROR', 'FATAL') ORDER BY timestamp;",
                     &result) == DuckDBError) {
        duckdb_destroy_result(&result);
        return;
    }
    idx_t rows = duckdb_row_count(&result);
    for (idx_t row = 0; row < rows; ++row) {
        char *message = duckdb_value_varchar(&result, 0, row);
        if (message) {
            fprintf(stderr, "catalog: %.600s\n", message);
            duckdb_free(message);
        }
    }
    duckdb_destroy_result(&result);
}

// One writer: connect, wait for the others, then commit `commits` times.
// Each insert autocommits, so each is one DuckLake transaction.
static void *commit_loop(void *argument) {
    writer_task *task = (writer_task *)argument;
    duckdb_connection connection = NULL;
    int failed = duckdb_connect(task->database, &connection) == DuckDBError;
    if (failed) {
        snprintf(task->error, STATEMENT_MAX, "writer %d could not connect", task->writer);
    }

    // Unconditional: a writer that failed to connect must not strand the
    // ones that did.
    pthread_barrier_wait(task->barrier);

    for (long long row = 0; !failed && row < task->commits; ++row) {
        char sql[STATEMENT_MAX];
        snprintf(sql, sizeof(sql), "INSERT INTO lake.main.writer_%d VALUES (%lld);",
                 task->writer, row);
        failed = run(connection, sql, task->error);
    }

    if (connection) {
        duckdb_disconnect(&connection);
    }
    return NULL;
}

static double seconds_between(struct timespec start, struct timespec end) {
    return (double)(end.tv_sec - start.tv_sec) + (double)(end.tv_nsec - start.tv_nsec) / 1e9;
}

int main(int argc, char **argv) {
    if (argc != 6 && argc != 7) {
        fprintf(stderr,
                "usage: %s <extension-dir> <extension> <attach-sql> <writers> <commits-each> "
                "[indexed]\n",
                argv[0]);
        return 2;
    }
    int indexed = argc == 7 && strcmp(argv[6], "indexed") == 0;
    const char *extension_dir = argv[1];
    const char *extension = argv[2];
    const char *attach = argv[3];
    int writers = atoi(argv[4]);
    long long commits = atoll(argv[5]);
    if (writers < 1 || commits < 1) {
        fprintf(stderr, "writers and commits-each must both be at least one\n");
        return 2;
    }

    duckdb_config config;
    if (duckdb_create_config(&config) == DuckDBError) {
        fprintf(stderr, "could not create a DuckDB config\n");
        return 1;
    }
    duckdb_set_config(config, "allow_unsigned_extensions", "true");
    // The lake lives under the attach, so the instance itself is in memory.
    duckdb_database database;
    char *open_error = NULL;
    if (duckdb_open_ext(NULL, &database, config, &open_error) == DuckDBError) {
        fprintf(stderr, "could not open DuckDB: %s\n", open_error ? open_error : "?");
        return 1;
    }
    duckdb_destroy_config(&config);

    duckdb_connection setup;
    if (duckdb_connect(database, &setup) == DuckDBError) {
        fprintf(stderr, "could not open the setup connection\n");
        return 1;
    }

    // One thread: DuckLake's own catalog cache races under intra-query
    // parallelism independently of the catalog backend, and a fresh attach
    // can then return an empty table list. The connections are the
    // concurrency under test, not the scheduler.
    char error[STATEMENT_MAX] = {0};
    char statement[STATEMENT_MAX];
    snprintf(statement, sizeof(statement), "SET extension_directory='%s';", extension_dir);
    int failed = run(setup, "SET threads=1;", error) || run(setup, statement, error) ||
                 run(setup, "INSTALL postgres;", error) || run(setup, "LOAD postgres;", error);
    if (!failed) {
        snprintf(statement, sizeof(statement), "LOAD '%s';", extension);
        failed = run(setup, statement, error) || run(setup, attach, error);
    }
    if (!failed) {
        // Warnings only, unless CONCURRENT_WRITERS_LOG names a level: a
        // failed burst reports what the catalog saw; a diagnostic run keeps
        // everything and writes it where CONCURRENT_WRITERS_LOG_FILE points.
        // Enabled for every type, since the catalog's is not one DuckDB
        // knows by name; the readers below filter on it.
        const char *level = getenv("CONCURRENT_WRITERS_LOG");
        char ignored[STATEMENT_MAX];
        snprintf(statement, sizeof(statement),
                 "CALL enable_logging(level => '%s', storage => 'memory');",
                 level ? level : "warn");
        run(setup, statement, ignored);
    }
    for (int writer = 0; !failed && writer < writers; ++writer) {
        snprintf(statement, sizeof(statement),
                 "CREATE TABLE lake.main.writer_%d (id BIGINT);", writer);
        failed = run(setup, statement, error);
        if (!failed && indexed) {
            snprintf(statement, sizeof(statement),
                     "CREATE INDEX writer_%d_by_id ON lake.main.writer_%d(id);", writer, writer);
            failed = run(setup, statement, error);
        }
    }
    if (failed) {
        fprintf(stderr, "setup: %s\n", error);
        return 1;
    }

    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, (unsigned)writers + 1);
    writer_task *tasks = calloc((size_t)writers, sizeof(writer_task));
    pthread_t *threads = calloc((size_t)writers, sizeof(pthread_t));
    if (!tasks || !threads) {
        fprintf(stderr, "out of memory\n");
        return 1;
    }
    for (int writer = 0; writer < writers; ++writer) {
        tasks[writer].database = database;
        tasks[writer].barrier = &barrier;
        tasks[writer].writer = writer;
        tasks[writer].commits = commits;
        pthread_create(&threads[writer], NULL, commit_loop, &tasks[writer]);
    }

    // Returns once every writer is connected and waiting.
    pthread_barrier_wait(&barrier);
    struct timespec started;
    clock_gettime(CLOCK_MONOTONIC, &started);
    for (int writer = 0; writer < writers; ++writer) {
        pthread_join(threads[writer], NULL);
    }
    struct timespec ended;
    clock_gettime(CLOCK_MONOTONIC, &ended);

    int any_failed = 0;
    for (int writer = 0; writer < writers; ++writer) {
        if (tasks[writer].error[0]) {
            fprintf(stderr, "writer %d: %s\n", writer, tasks[writer].error);
            any_failed = 1;
        }
    }
    const char *log_file = getenv("CONCURRENT_WRITERS_LOG_FILE");
    if (log_file) {
        char ignored[STATEMENT_MAX];
        snprintf(statement, sizeof(statement),
                 "COPY (SELECT timestamp, log_level, message FROM duckdb_logs WHERE type = 'moraine' "
                 "ORDER BY timestamp) TO '%s' (FORMAT csv);",
                 log_file);
        run(setup, statement, ignored);
    }
    if (any_failed) {
        report_catalog_warnings(setup);
        return 1;
    }
    printf("elapsed_seconds=%.6f\n", seconds_between(started, ended));

    free(tasks);
    free(threads);
    duckdb_disconnect(&setup);
    duckdb_close(&database);
    return 0;
}
