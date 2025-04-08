use clap::Parser;
use query_exec::MemPool;
use query_exec::{
    prelude::{execute, load_db, to_logical, to_physical, MemoryPolicy, OnDiskPipelineGraph},
    BufferPool, ContainerId, OnDiskStorage,
};
use std::sync::Arc;
use fbtree::container::ContainerManager;

#[derive(Debug, Parser)]
#[clap(
    name = "Sort Benchmark",
    version = "1.0",
    author = "C",
    about = "Benchmarking sort execution for TPCH queries."
)]
struct SortOpt {
    /// Buffer pool size (number of frames)
    #[clap(short = 'b', long = "buffer-pool-size", default_value = "10000")]
    buffer_pool_size: usize,

    /// Query ID to benchmark (e.g., 100)
    #[clap(short = 'q', long = "query", default_value = "100")]
    query_id: u32,

    /// Memory size per operator
    #[clap(short = 'm', long = "memory-size", default_value = "100")]
    memory_size: usize,

    /// Number of iterations for the benchmark
    #[clap(short = 'n', long = "num-iterations", default_value = "1")]
    num_iterations: usize,

    /// Path to the buffer pool directory
    #[clap(short = 'p', long = "path", default_value = "bp-dir-tpch-sf-1")]
    path: String,
}

fn run_sort(memory_size: usize, bp: Arc<BufferPool>, query_id: u32) -> Result<(), String> {
    let temp_c_id = 100017;
    let exclude_last_pipeline = true;

    // Load the on-disk storage from the BufferPool.
    let storage = Arc::new(OnDiskStorage::load(&bp));

    // Load the database named "TPCH".
    let (db_id, catalog) =
        load_db(&storage, "TPCH").map_err(|e| format!("Failed to load DB: {:?}", e))?;

    // Construct the path to the SQL query file.
    let query_path = format!("tpch/queries/q{}.sql", query_id);
    let sql_string = std::fs::read_to_string(&query_path)
        .map_err(|e| format!("Failed to read SQL file {}: {}", query_path, e))?;

    // Convert the SQL string into a logical expression.
    let logical = to_logical(db_id, &catalog, &sql_string)
        .map_err(|e| format!("Failed to convert to logical: {:?}", e))?;
    // Convert the logical plan to a physical plan.
    let physical = to_physical(logical);

    // Set a memory policy for query execution.
    let mem_policy = Arc::new(MemoryPolicy::FixedSizeLimit(memory_size));

    // Create the pipeline graph for executing the query.
    let exe = OnDiskPipelineGraph::new(
        db_id,
        temp_c_id as ContainerId,
        &catalog,
        &storage,
        &bp,
        &mem_policy,
        physical.clone(),
        exclude_last_pipeline,
    );

    // Execute the pipeline.
    let _result = execute(db_id, &storage, exe, false);
    println!("stats after {}", bp.stats());

    println!("Sort execution completed successfully.");

    Ok(())
}

fn get_bp(dir: &str, num_frames: usize) -> Arc<BufferPool> {
    let cm = Arc::new(ContainerManager::new(&dir, true, false).unwrap());
    let bp = Arc::new(BufferPool::new(num_frames, cm).unwrap());
    bp
}

fn main() {
    // Parse command-line arguments.
    let opt = SortOpt::parse();
    // Initialize the BufferPool using the provided parameters.
    let bp = get_bp(&opt.path, opt.buffer_pool_size);
    println!("bp size {:?}", opt.buffer_pool_size);
    // Run the sort benchmark for the specified number of iterations.
    for itr in 0..opt.num_iterations {
        println!("Iteration {}", itr + 1);
        if let Err(e) = run_sort(opt.memory_size, bp.clone(), opt.query_id) {
            eprintln!("Error during sort execution: {}", e);
            std::process::exit(1);
        }
    }
    let _ = bp.clear_dirty_flags();
    let _ = bp.flush_all_and_reset();
}
