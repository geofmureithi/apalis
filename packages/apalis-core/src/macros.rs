#[cfg(feature = "tracing")]
#[allow(unused_macros)]
macro_rules! debug {
    ($($tt:tt)*) => {
        tracing::debug!($($tt)*)
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
macro_rules! debug {
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
/// use apalis_core::features_table;
/// # fn example() -> &'static str {
/// features_table! {
///     setup = "MemoryStorage::new();",
///     TaskSink => supported("Ability to push new tasks", true),
///     Serialization => limited("Serialization support for arguments. Only accepts `json`"),
///     FetchById => not_implemented("Allow fetching a task by its ID"),
///     RegisterWorker => not_supported("Allow registering a worker with the backend"),
/// }
/// # }
///
/// ```
#[macro_export]
macro_rules! features_table {
    (
        setup = $setup:literal,
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
            "<summary>Tests:</summary>\n",
            "\n",
            $(
                features_table!(@maybe_generate_doctest $setup, $feature, $status $(, $include_example)?)
            ),*,
            "</details>\n"
        )
    };

    // Format feature names ONLY - no code, just name and link
    (@format_feature_name WebUI) => {
        concat!("[`Web Interface`](https://docs.rs/apalis-board-api/)")
    };

    (@format_feature_name Serialization) => {
        concat!("[`Serialization`](https://docs.rs/apalis-core/latest/apalis_core/backend/codec/index.html)")
    };

    (@format_feature_name Workflow) => {
        concat!("[`Workflow`](https://docs.rs/apalis-workflow/)")
    };

    (@format_feature_name $feature:ident) => {
        concat!("[`", stringify!($feature), "`](https://docs.rs/apalis-core/latest/apalis_core/backend/trait.", stringify!($feature), ".html)")
    };
    (@format_feature_name $feature:literal) => {
        $feature
    };

    // Helper to decide whether to generate doctest - with explicit flag
    (@maybe_generate_doctest $setup:literal, $feature:tt, $status:ident, $include_example:tt) => {
        features_table!(@check_should_generate $setup, $feature, $status, $include_example)
    };

    // Helper to decide whether to generate doctest - without explicit flag (default behavior)
    (@maybe_generate_doctest $setup:literal, $feature:tt, $status:ident) => {
        features_table!(@check_should_generate $setup, $feature, $status, default)
    };

    // Check if we should generate - true flag
    (@check_should_generate $setup:literal, $feature:tt, $status:ident, true) => {
        features_table!(@do_generate_doctest $setup, $feature, $status)
    };

    // Check if we should generate - false flag
    (@check_should_generate $setup:literal, $feature:tt, $status:ident, false) => {
        ""
    };

    // Check if we should generate - default behavior
    (@check_should_generate $setup:literal, $feature:tt, supported, default) => {
        features_table!(@do_generate_doctest $setup, $feature, supported)
    };
    (@check_should_generate $setup:literal, $feature:tt, limited, default) => {
        features_table!(@do_generate_doctest $setup, $feature, limited)
    };
    (@check_should_generate $setup:literal, $feature:tt, not_implemented, default) => {
        ""
    };
    (@check_should_generate $setup:literal, $feature:tt, not_supported, default) => {
        ""
    };

    // Actually generate the doctest
    (@do_generate_doctest $setup:literal, $feature:ident, $status:ident) => {
        concat!(
            "#### ", stringify!($feature), "\n\n",
            "```rust\n",
            "# use apalis_core::worker::context::WorkerContext;\n",
            "# use apalis_core::worker::builder::WorkerBuilder;\n",
            "#[tokio::main]\n",
            "async fn main() {\n",
            "    // let mut backend = /* snip */;\n",
            "    # let mut backend = ", $setup, "\n",
            features_table!(@assert_function $feature), "\n",
            "```\n\n"
        )
    };

    (@assert_function Backend) => { concat!(
        "    # use futures_util::StreamExt; \n",
        "    # use apalis_core::backend::Backend;\n",
        "    async fn task(task: u32, worker: WorkerContext) {\n",
        "        // Do some work \n",
        "    #    worker.stop().unwrap();\n",
        "    }\n",
        "    let worker = WorkerBuilder::new(\"rango-tango\")\n",
        "        .backend(backend)\n",
        "        .build(task);\n",
        "    let _ = worker.stream().take(1).collect::<Vec<_>>().await;\n",
        "}\n"
    ) };
    (@assert_function TaskSink) => { concat!(
        "    # use apalis_core::backend::TaskSink;\n",
        "    backend.push(42).await.unwrap();\n\n",
        "    async fn task(task: u32, worker: WorkerContext) {\n",
        "        worker.stop().unwrap();\n",
        "    }\n",
        "    let worker = WorkerBuilder::new(\"rango-tango\")\n",
        "        .backend(backend)\n",
        "        .build(task);\n",
        "    worker.run().await.unwrap();\n",
        "}\n"
    ) };
    (@assert_function MakeShared) => { "fn assert_make_shared<T: Clone + Send + 'static>(t: T); assert_make_shared(backend);" };
    // Standardized assert function mapping for identifiers
    (@assert_function Workflow) => { concat!(
        "    # use apalis_workflow::*;\n",
        "    backend.push_start(42).await.unwrap();\n\n",
        "    async fn task1(task: u32, worker: WorkerContext) -> u32 {\n",
        "        task + 99 \n",
        "    }\n",
        "    async fn task2(task: u32, worker: WorkerContext) -> u32 {\n",
        "        task + 1 \n",
        "    }\n",
        "    async fn task3(task: u32, worker: WorkerContext) {\n",
        "        assert_eq!(task, 142);\n",
        "        worker.stop().unwrap();\n",
        "    }\n",
        "    let workflow = Workflow::new(\"test_workflow\")\n",
        "       .then(task1)\n",
        "       .then(task2)\n",
        "       .then(task3);\n",
        "    let worker = WorkerBuilder::new(\"rango-tango\")\n",
        "        .backend(backend)\n",
        "        .build(workflow);\n",
        "    worker.run().await.unwrap();\n",
        "}\n"
    ) };
    (@assert_function Serialization) => { concat!(
        "    # use apalis_core::backend::codec::Codec;\n",
        "    # use apalis_core::backend::Backend;\n",
        "   fn assert_codec<B: Backend>(backend: B) \n",
        "   where\n",
        "       B::Codec: Codec<(), Compact=Vec<u8>>,\n",
        "   {\n",
        "   }\n",
        "   assert_codec(backend);\n",
        "}\n"
    ) };
    (@assert_function WaitForCompletion) => { concat!(
        "# use apalis_core::backend::WaitForCompletion;\n",
        "    fn assert_wait_for_completion<B: WaitForCompletion<(), Args = u32>>(backend: B) {};\n",
        "    assert_wait_for_completion(backend);\n",
        "}\n"
    ) };
    (@assert_function WebUI) => { concat!(
        "# use apalis_core::backend::Expose;\n",
        "    fn assert_web_ui<B: Expose<u32>>(backend: B) {};\n",
        "    assert_web_ui(backend);\n",
        "}\n"
    ) };
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
            setup = "{
                use apalis_core::backend::memory::MemoryStorage;
                // No migrations
                MemoryStorage::new()
            };",
            TaskSink => supported("Ability to push new tasks", true),
            Serialization => limited("Serialization support for arguments. Only accepts `json`", false),
            FetchById => not_implemented("Allow fetching a task by its ID"),
            RegisterWorker => not_supported("Allow registering a worker with the backend"),
        };

        println!("Generated table:\n{}", table);

        // Table should have proper structure
        assert!(table.contains("## Features"));
        assert!(table.contains("| Feature | Status | Description"));
        assert!(table.contains("|---------|--------|------------------------"));

        // Table rows should contain ONLY name, status, description
        assert!(table.contains("| ✅ | Ability to push new tasks |"));

        // Code examples should be in separate sections for features with true flag or no flag (default)
        assert!(table.contains("#### TaskSink"));

        // Serialization should NOT have example because it has explicit false flag
        assert!(!table.contains("#### Serialization"));

        // Non-supported features should NOT have example sections
        assert!(!table.contains("#### FetchById"));
        assert!(!table.contains("#### RegisterWorker"));

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
            setup = "{ unreachable!() }",
            TaskSink => supported("Ability to push new tasks", false),
        };

        // Should not contain any examples
        assert!(!table.contains("#### TaskSink Example"));
        assert!(!table.contains("```rust"));
    }

    #[test]
    fn test_explicit_true_flag() {
        let table = features_table! {
            setup = "{ unreachable!() }",
            TaskSink => supported("Ability to push new tasks", true),
            Serialization => limited("Serialization support", true),
        };

        // Should contain examples for both
        assert!(table.contains("#### TaskSink"));
        assert!(table.contains("#### Serialization"));
    }
}
