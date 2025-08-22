use futures::channel::mpsc::{self, Receiver};
use futures::{SinkExt, StreamExt};
use libsqlite3_sys::{sqlite3, sqlite3_update_hook};
use sqlx::SqlitePool;
use sqlx::{sqlite::SqliteConnection, Connection, Executor};
use std::ffi::{c_void, CStr};
use std::os::raw::{c_char, c_int};
use std::ptr;

#[derive(Debug)]
struct DbEvent {
    op: &'static str,
    db_name: String,
    table_name: String,
    rowid: i64,
}

// Callback for SQLite update hook
extern "C" fn update_hook_callback(
    arg: *mut c_void,
    op: c_int,
    db_name: *const c_char,
    table_name: *const c_char,
    rowid: i64,
) {
    let op_str = match op {
        libsqlite3_sys::SQLITE_INSERT => "INSERT",
        libsqlite3_sys::SQLITE_UPDATE => "UPDATE",
        libsqlite3_sys::SQLITE_DELETE => "DELETE",
        _ => "UNKNOWN",
    };

    unsafe {
        let db = CStr::from_ptr(db_name).to_string_lossy().to_string();
        let table = CStr::from_ptr(table_name).to_string_lossy().to_string();

        // Recover sender from raw pointer
        let tx = &mut *(arg as *mut mpsc::UnboundedSender<DbEvent>);

        // Ignore send errors (receiver closed)
        let _ = tx.send(DbEvent {
            op: op_str,
            db_name: db,
            table_name: table,
            rowid,
        });
    }
}

pub struct PushListener {
    pool: SqlitePool,
    rx: Receiver<DbEvent>,
}

async fn run_it() -> Result<(), sqlx::Error> {
    let (tx, mut rx) = mpsc::channel::<DbEvent>(10);

    let pool = SqlitePool::connect(":memory:").await.unwrap();

    let mut conn = pool.acquire().await?;

    // Create table
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .await?;

    // Get raw sqlite3* handle
    let handle: *mut sqlite3 = conn.lock_handle().await.unwrap().as_raw_handle().as_ptr();

    // Put sender in a Box so it has a stable memory address
    let tx_box = Box::new(tx);
    let tx_ptr = Box::into_raw(tx_box) as *mut c_void;

    unsafe {
        sqlite3_update_hook(handle, Some(update_hook_callback), tx_ptr);
    }

    // Spawn a task to process events asynchronously
    tokio::spawn(async move {
        while let Some(event) = rx.next().await {
            println!("Got DB event: {:?}", event);
        }
    });

    // These will trigger the hook
    conn.execute("INSERT INTO users (name) VALUES ('Alice')")
        .await?;
    conn.execute("INSERT INTO users (name) VALUES ('Bob')")
        .await?;
    conn.execute("UPDATE users SET name = 'Charlie' WHERE id = 1")
        .await?;
    conn.execute("DELETE FROM users WHERE id = 2").await?;

    // Let events flush
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    Ok(())
}
