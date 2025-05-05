use clap::Parser;
use fbtree::container::ContainerManager;
use query_exec::{
    prelude::{create_db, create_table_from_sql, import_csv, Catalog},
    BufferPool, ContainerDS, OnDiskStorage,
};
use std::time::Instant;
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use sysinfo::{CpuExt, ProcessExt, System, SystemExt};

#[derive(Debug, Parser)]
#[clap(name = "TPC-H Benchmark", about = "Benchmarking query_exec vs DuckDB.")]
pub struct TpchOpt {
    #[clap(
        short = 'f',
        long = "file-source",
        default_value = "yellow_tripdata_2024-01"
    )]
    pub file_source: Box<str>,

    /// Buffer pool size.
    #[clap(short = 'b', long = "buffer-pool-size", default_value = "1000000")]
    // 4K * 1000000 = 4GB
    pub buffer_pool_size: usize,

    /// Number of iterations for the benchmark
    #[clap(short = 'n', long = "num-iterations", default_value = "10")]
    pub num_iterations: usize,
}

fn get_catalog() -> Arc<Catalog> {
    Arc::new(Catalog::new())
}

fn get_bp(dir: &str, num_frames: usize) -> Arc<BufferPool> {
    let cm = Arc::new(ContainerManager::new(&dir, true, false).unwrap());
    let bp = Arc::new(BufferPool::new(num_frames, cm).unwrap());
    bp
}

fn get_system_metrics(system: &mut System) -> (f32, f32) {
    system.refresh_all();
    let cpu_usage = system.global_cpu_info().cpu_usage();
    let memory = system.used_memory() as f32 / 1024.0; // Convert to MB
    (cpu_usage, memory)
}

fn main() {
    let parse = TpchOpt::parse();
    let opt = parse;

    // Initialize system info
    let mut system = System::new_all();

    // Define the data directory based on scale factor
    let data_dir = format!("data/{}.csv", opt.file_source);
    if !PathBuf::from(&data_dir).exists() {
        panic!(
            "Data directory {} does not exist. Generate data first.",
            data_dir
        );
    } else {
        println!("Importing data from {}.", data_dir);
    }

    let catalog = get_catalog();
    let bp_name = format!("bp-dir-{}", opt.file_source);

    if PathBuf::from(&bp_name).exists() {
        panic!("Buffer pool directory {} already exists.", bp_name);
    }

    let bp = get_bp(&bp_name, opt.buffer_pool_size);

    let storage = Arc::new(OnDiskStorage::new(&bp));
    let db_id = create_db(&storage, "taxi").unwrap();

    let table_names = vec![
        "yellow_tripdata", // Your new table
                           // Add other tables if needed
    ];

    println!("Creating tables...");

    let mut tables = HashMap::new();

    for table in &table_names {
        let path = format!("data/tables/{}.sql", table);
        let sql = std::fs::read_to_string(&path).expect(&format!("Failed to read {}", path));
        let c_id = create_table_from_sql(
            &catalog,
            &storage,
            db_id,
            sql.as_ref(),
            ContainerDS::AppendOnly,
        )
        .expect(&format!("Failed to create table {}", table));
        tables.insert(table, c_id);
    }

    println!("Tables created. Loading data...");

    for table in &table_names {
        let c_id = tables[table];
        let path = format!("yellow/{}.csv", opt.file_source);
        import_csv(&catalog, &storage, db_id, c_id, &path, true, b',')
            .expect(&format!("Failed to import data for table {}", table));
    }

    println!("Data loaded. Starting benchmark...");

    for table in &table_names {
        println!("Benchmarking table: {}", table);
        let c_id = tables[table];

        for itr in 0..opt.num_iterations {
            // Capture metrics before operation
            let (cpu_start, mem_start) = get_system_metrics(&mut system);
            println!(
                "Iteration {}: Start - CPU: {:.2}%, Memory: {:.2} MB",
                itr + 1,
                cpu_start,
                mem_start
            );

            let start_time = Instant::now();

            // Execute the query
            // Assuming `query_exec` has a method to perform SQL queries
            // Modify this part based on your actual implementation
            // Example:
            // let result = execute_query(&catalog, &storage, db_id, "SELECT VendorID, SUM(total_amount) AS total_amount FROM yellow_tripdata GROUP BY VendorID").unwrap();
            // println!("{:?}", result);

            // Placeholder for query execution
            // Replace with actual query execution code
            // For demonstration, we'll assume a function `execute_query` exists
            // let result = execute_query(...);
            // println!("{:?}", result);

            // Simulate query execution with sleep (remove in actual implementation)
            std::thread::sleep(std::time::Duration::from_secs(2));

            let duration = start_time.elapsed();

            // Capture metrics after operation
            let (cpu_end, mem_end) = get_system_metrics(&mut system);
            println!(
                "Iteration {}: Post_In_Memory - CPU: {:.2}%, Memory: {:.2} MB, Runtime: {:.2?} seconds",
                itr + 1,
                cpu_end,
                mem_end,
                duration
            );

            // Optionally, log these metrics to a file or external system
            // Example:
            // print_log(log_prefix="benchmark", engine="query_exec", itr=itr+1, phase="Post_In_Memory", operation_type="sum_of_total_amount", cpu=cpu_end, memory=mem_end, runtime=duration.as_secs_f32());
        }
    }

    println!("Benchmarking completed.");
}
