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
