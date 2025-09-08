#[cfg(feature = "tracing")]
#[allow(unused_macros)]
macro_rules! trace {
    ($($tt:tt)*) => {
        tracing::trace!($($tt)*)
    }
}

#[cfg(feature = "tracing")]
#[allow(unused_macros)]
macro_rules! info {
    ($($tt:tt)*) => {
        tracing::info!($($tt)*)
    }
}

#[cfg(feature = "tracing")]
#[allow(unused_macros)]
macro_rules! error {
    ($($tt:tt)*) => {
        tracing::error!($($tt)*)
    };
}

#[cfg(not(feature = "tracing"))]
#[allow(unused_macros)]
macro_rules! trace {
    ($($tt:tt)*) => {};
}

#[cfg(not(feature = "tracing"))]
#[allow(unused_macros)]
macro_rules! info {
    ($($tt:tt)*) => {};
}

#[cfg(not(feature = "tracing"))]
#[allow(unused_macros)]
macro_rules! error {
    ($($tt:tt)*) => {};
}

/// Macro to generate feature tables for backend documentation with standardized assert functions
///
/// The table contains ONLY feature names, status icons, and descriptions.
/// Code examples appear as separate sections below the table.
///
/// Usage:
/// ```rust
/// features_table! {
///     setup = MemoryStorage::new();,
///     TaskSink => supported("Ability to push new tasks", true),
///     Codec => limited("Serialization support for arguments. Only accepts `json`"),
///     Acknowledge => supported("In-built acknowledgement after task completion", true),
///     FetchById => not_implemented("Allow fetching a task by its ID"),
///     RegisterWorker => not_supported("Allow registering a worker with the backend"),
/// }
/// ```
#[macro_export]
macro_rules! features_table {
    (
        setup = $setup:expr;,
        $(
            $feature:tt => $status:ident($description:literal $(, $include_example:tt)?)
        ),* $(,)?
    ) => {
        concat!(
            "## Features\n",
            "| Feature | Status | Description            |\n",
            "|---------|--------|------------------------|\n",
            $(
                "| ",
                features_table!(@format_feature_name $feature),
                " | ",
                features_table!(@status_icon $status),
                " | ",
                $description,
                " |\n"
            ),*,
            "\n",
            "Key:\n",
            "✅ : Supported | ⚠️ : Not implemented | ❌ : Not Supported | ❗ Limited support\n\n",
            "<details>\n",
            "<summary>Examples:</summary>\n",
            "\n",
            $(
                features_table!(@maybe_generate_doctest $setup, $feature, $status $(, $include_example)?)
            ),*,
            "</details>\n"
        )
    };

    // Format feature names ONLY - no code, just name and link
    (@format_feature_name $feature:ident) => {
        concat!("[`", stringify!($feature), "`](apalis_core::backend::", stringify!($feature), ")")
    };
    (@format_feature_name $feature:literal) => {
        concat!("[`", $feature, "`]")
    };

    // Helper to decide whether to generate doctest - with explicit flag
    (@maybe_generate_doctest $setup:expr, $feature:tt, $status:ident, $include_example:tt) => {
        features_table!(@check_should_generate $setup, $feature, $status, $include_example)
    };

    // Helper to decide whether to generate doctest - without explicit flag (default behavior)
    (@maybe_generate_doctest $setup:expr, $feature:tt, $status:ident) => {
        features_table!(@check_should_generate $setup, $feature, $status, default)
    };

    // Check if we should generate - true flag
    (@check_should_generate $setup:expr, $feature:tt, $status:ident, true) => {
        features_table!(@do_generate_doctest $setup, $feature, $status)
    };

    // Check if we should generate - false flag
    (@check_should_generate $setup:expr, $feature:tt, $status:ident, false) => {
        ""
    };

    // Check if we should generate - default behavior
    (@check_should_generate $setup:expr, $feature:tt, supported, default) => {
        features_table!(@do_generate_doctest $setup, $feature, supported)
    };
    (@check_should_generate $setup:expr, $feature:tt, limited, default) => {
        features_table!(@do_generate_doctest $setup, $feature, limited)
    };
    (@check_should_generate $setup:expr, $feature:tt, not_implemented, default) => {
        ""
    };
    (@check_should_generate $setup:expr, $feature:tt, not_supported, default) => {
        ""
    };

    // Actually generate the doctest
    (@do_generate_doctest $setup:expr, $feature:ident, $status:ident) => {
        concat!(
            "#### ", stringify!($feature), " Example\n\n",
            "```rust\n",
            "use crate::backend::", stringify!($feature), ";\n",
            "#[tokio::main]\n",
            "async fn main() {\n",
            "    let mut backend = ", stringify!($setup), ";\n\n",
            features_table!(@assert_function $feature), "\n",
            "```\n\n"
        )
    };

    // Standardized assert function mapping for identifiers
    (@assert_function TaskSink) => { concat!(
        "    async fn task(task: u32, worker: WorkerContext) {\n",
        "        tokio::time::sleep(Duration::from_secs(1)).await;\n",
        "        worker.stop().unwrap();\n",
        "    }\n",
        "    let worker = WorkerBuilder::new(\"rango-tango\")\n",
        "        .backend(backend)\n",
        "        .build(task);\n",
        "    worker.run().await.unwrap();\n",
        "}\n"
    ) };
    (@assert_function Codec) => { "assert_codec(backend);" };
    (@assert_function Acknowledge) => { "assert_acknowledge(backend);" };
    (@assert_function FetchById) => { "assert_fetch_by_id(backend);" };
    (@assert_function RegisterWorker) => { "assert_register_worker(backend);" };
    (@assert_function PipeExt) => { "assert_pipe_ext(backend);" };
    (@assert_function Sharable) => { "assert_sharable(backend);" };
    (@assert_function Workflow) => { "assert_workflow(backend);" };
    (@assert_function WaitForCompletion) => { "assert_wait_for_completion(backend);" };

    // Status icons
    (@status_icon supported) => { "✅" };
    (@status_icon limited) => { "✅ ❗" };
    (@status_icon not_implemented) => { "⚠️" };
    (@status_icon not_supported) => { "❌" };
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_clean_table_structure_with_example_flags() {
        let table = features_table! {
            setup = MemoryStorage::new();,
            TaskSink => supported("Ability to push new tasks", true),
            Codec => limited("Serialization support for arguments. Only accepts `json`", false),
            Acknowledge => supported("In-built acknowledgement after task completion"),
            FetchById => not_implemented("Allow fetching a task by its ID"),
            RegisterWorker => not_supported("Allow registering a worker with the backend"),
        };

        println!("Generated table:\n{}", table);

        // Table should have proper structure
        assert!(table.contains("## Features"));
        assert!(table.contains("| Feature | Status | Description"));
        assert!(table.contains("|---------|--------|------------------------"));

        // Table rows should contain ONLY name, status, description
        assert!(table.contains(
            "| [`TaskSink`](crate::backend::TaskSink) | ✅ | Ability to push new tasks |"
        ));
        assert!(table.contains("| [`Codec`](crate::backend::Codec) | ✅ ❗ | Serialization support for arguments. Only accepts `json` |"));

        // Code examples should be in separate sections for features with true flag or no flag (default)
        assert!(table.contains("#### TaskSink Example"));
        assert!(table.contains("#### Acknowledge Example"));

        // Codec should NOT have example because it has explicit false flag
        assert!(!table.contains("#### Codec Example"));

        // Non-supported features should NOT have example sections
        assert!(!table.contains("#### FetchById Example"));
        assert!(!table.contains("#### RegisterWorker Example"));

        // Verify table cells don't contain code blocks
        let lines: Vec<&str> = table.lines().collect();
        for line in lines {
            if line.starts_with("|") && line.contains("TaskSink") {
                // This table row should NOT contain ```rust
                assert!(
                    !line.contains("```rust"),
                    "Table row contains code block: {}",
                    line
                );
            }
        }
    }

    #[test]
    fn test_explicit_false_flag() {
        let table = features_table! {
            setup = MemoryStorage::new();,
            TaskSink => supported("Ability to push new tasks", false),
            Acknowledge => limited("In-built acknowledgement", false),
        };

        // Should not contain any examples
        assert!(!table.contains("#### TaskSink Example"));
        assert!(!table.contains("#### Acknowledge Example"));
        assert!(!table.contains("```rust"));
    }

    #[test]
    fn test_explicit_true_flag() {
        let table = features_table! {
            setup = MemoryStorage::new();,
            TaskSink => supported("Ability to push new tasks", true),
            Codec => limited("Serialization support", true),
        };

        // Should contain examples for both
        assert!(table.contains("#### TaskSink Example"));
        assert!(table.contains("#### Codec Example"));
    }
}
