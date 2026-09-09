//! Delete-file lifetime across an interrupted standalone commit.

mod transactions;

use std::{
    io::Write,
    path::Path,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use moraine::{Catalog, CatalogOptions, CatalogSnapshot};
use object_store::local::LocalFileSystem;

use crate::helpers::*;

const FLUSH_INTERVAL: Duration = Duration::from_secs(10);
const DEADLINE: Duration = Duration::from_secs(30);

struct Session(Child);

impl Drop for Session {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn snapshot(store: &Path) -> Arc<CatalogSnapshot> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let store = Arc::new(LocalFileSystem::new_with_prefix(store).unwrap());
        let catalog = Catalog::open_read_only(store, CatalogOptions::default())
            .await
            .unwrap();
        let snapshot = catalog.snapshot().await.unwrap();
        catalog.close().await.unwrap();
        snapshot
    })
}

fn wait_until(mut ready: impl FnMut() -> bool, description: &str) {
    let deadline = Instant::now() + DEADLINE;
    while !ready() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {description}"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// An interrupted commit can still land; its registered delete file must
/// survive.
#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn an_interrupted_located_deletion_keeps_its_committed_file() {
    interrupted_deletion_retains_files(true);
}

#[test]
#[ignore = "needs the downloaded DuckDB CLI and packaged Moraine extension"]
fn an_interrupted_sql_deletion_keeps_its_committed_file() {
    interrupted_deletion_retains_files(false);
}

#[allow(clippy::too_many_lines)]
fn interrupted_deletion_retains_files(located: bool) {
    let store = TempDir::new("delete-located-interrupted-store");
    let data = TempDir::new("delete-located-interrupted-data");
    let options = format!(
        ", META_DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0",
        data.path().display()
    );
    run_ducklake_sql_with_options(
        store.path(),
        data.path(),
        &options,
        "CREATE TABLE lake.main.t(a BIGINT); INSERT INTO lake.main.t VALUES (1), (2), (3);",
    );
    let before = snapshot(store.path());
    let main = before.schema_by_name("main").unwrap().id;
    let table = before.table_by_name(main, "t").unwrap().id;
    let file = before.data_files_of(table).pop().unwrap();

    let mut session = Session(
        Command::new(cli_path())
            .args(["-unsigned", "-csv"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let deletion = if located {
        format!(
            "CALL moraine_delete_located('lake', 'main', 't', [{{row_id: {}::BIGINT, data_file_id: {}::UBIGINT}}]);",
            file.row_id_start.unwrap(),
            file.id.get()
        )
    } else {
        "DELETE FROM lake.main.t WHERE a = 1;".to_owned()
    };
    // The long spacing withholds durability only from a commit inside it,
    // so a first commit opens the window the deletion is then caught in.
    let sql = format!(
        "SET threads=1;\n{}\n\
         ATTACH 'ducklake:moraine:{}' AS lake (DATA_PATH '{}'{options}, META_FLUSH_INTERVAL_MS {});\n\
         CREATE TABLE lake.main.warm(a INTEGER);\n\
         {deletion}\n",
        load_statement(),
        store.path().display(),
        data.path().display(),
        FLUSH_INTERVAL.as_millis(),
    );
    let submitted_at = Instant::now();
    let input = session.0.stdin.as_mut().unwrap();
    input.write_all(sql.as_bytes()).unwrap();
    input.flush().unwrap();

    wait_until(
        || parquet_files_under(data.path()).len() == 2,
        "the delete file to be written",
    );
    // Allow staging to finish while the long WAL cadence withholds durability.
    std::thread::sleep(Duration::from_secs(1));
    assert!(
        submitted_at.elapsed() < FLUSH_INTERVAL / 2,
        "staging missed the interrupt window"
    );
    assert!(
        Command::new("kill")
            .args(["-INT", &session.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );

    wait_until(
        || snapshot(store.path()).delete_files_of(table).len() == 1,
        "the interrupted commit to become durable",
    );
    let deletes = snapshot(store.path()).delete_files_of(table);
    assert_eq!(deletes.len(), 1, "the interrupted commit must have landed");
    assert!(
        parquet_files_under(data.path()).iter().any(|path| path
            .file_name()
            .unwrap()
            .to_string_lossy()
            == deletes[0].path),
        "the interrupted commit registered a delete file that cleanup removed"
    );

    drop(session.0.stdin.take());
    wait_until(
        || session.0.try_wait().unwrap().is_some(),
        "the interrupted session to close",
    );
    if located {
        assert!(
            !session.0.try_wait().unwrap().unwrap().success(),
            "the standalone call must have been interrupted before acknowledgement"
        );
    }
    assert_eq!(
        csv_rows(&run_ducklake_sql(
            store.path(),
            data.path(),
            "SELECT a FROM lake.main.t ORDER BY a;"
        )),
        vec![vec!["2".to_owned()], vec!["3".to_owned()]],
        "a fresh attach must read the committed deletion"
    );
}
