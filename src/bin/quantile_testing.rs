use core::num;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use csv::Writer;
use fbtree::container::ContainerManager;

use serde::Deserialize;

use query_exec::{
    prelude::{
        load_db, quantile_generation_execute, to_logical, to_physical, DataType, MemoryPolicy,
        OnDiskPipelineGraph,
    },
    quantile_lib::QuantileMethod,
    BufferPool, ContainerId, OnDiskStorage,
};

// A small struct to hold MSE and max-abs-error:
#[derive(Debug)]
struct ErrorMetrics {
    mse: f64,
    max_abs: f64,
}

// Where we'll store final data: (method, ds, query) -> ErrorMetrics
type ResultMap = HashMap<(String, String, u32), ErrorMetrics>;

fn main() -> Result<(), String> {
    // ---------------------------
    // 1) Setup config
    // ---------------------------
    let data_sources = vec!["yellow-tripdata-2024-01"];
    let data_sources = vec!["tpch-sf-1"];
    let queries = vec![100, 101];
    let methods = vec![
        QuantileMethod::Mean,
        QuantileMethod::GKSketch,
        // QuantileMethod::Median,
        // QuantileMethod::Sampling,
        // QuantileMethod::Histograms,
    ];

    let num_quantiles = 10;
    // For buffer pool / memory
    let bp_path = "bp-dir-";
    let buffer_pool_size = 10_000;
    let memory_size = 100;

    // We'll store results for each (method, ds, query).
    let mut results_map: ResultMap = HashMap::new();

    // ---------------------------
    // 2) Outer loop: method
    // ---------------------------
    // For each method, loop over data_sources and queries
    for ds in &data_sources {
        let bp_joined = format!("{}{}", bp_path, ds);
        let cm = Arc::new(ContainerManager::new(&bp_joined.clone(), true, false).unwrap());
        let bp = Arc::new(BufferPool::new(buffer_pool_size, cm).unwrap());
        // Create directory: e.g. "quantile_data/TPCH_SF1" if needed
        let base_path = format!("quantile_data/{}", ds);
        fs::create_dir_all(&base_path)
            .map_err(|e| format!("Failed to create directory {}: {:?}", base_path, e))?;

        for &query_id in &queries {
            println!(
                "executing query {} for data source {}",
                query_id, &bp_joined
            );
            // (A) Run the pipeline for (method, ds, query_id):
            if let Err(e) = run_quantile_generation(
                memory_size,
                bp.clone(),
                query_id,
                &methods,
                num_quantiles,
                ds,
                &base_path,
            ) {
                eprintln!(
                    "Error: method={:?}, ds={}, q={}: {}",
                    methods, ds, query_id, e
                );
                // decide whether to exit or keep going
                return Err(e);
            }

            // (B) The code above will write "q<query_id>_<method>_eval.json"
            //     We can parse it to get MSE & max_err:
        }
    }

    for method in methods.clone() {
        for ds in &data_sources {
            let base_path = format!("quantile_data/{}", ds);
            for &query_id in &queries {
                let method_str = method.to_string();
                let eval_path = format!("{}/q{}_{}_eval.json", base_path, query_id, method_str);
                match parse_evaluation_json(&eval_path) {
                    Ok((mse, max_abs)) => {
                        // Insert into our results map
                        results_map.insert(
                            (
                                format!("{} {} quantiles", method_str.clone(), num_quantiles),
                                ds.to_string(),
                                query_id,
                            ),
                            ErrorMetrics { mse, max_abs },
                        );
                    }
                    Err(err) => {
                        eprintln!("Could not parse eval JSON: {} => {}", eval_path, err);
                    }
                }
            }
        }
    }

    // ---------------------------
    // 3) Build the CSV in "wide" format
    // ---------------------------
    let out_csv = "quantile_data/wide_results.csv";
    let mut wtr =
        Writer::from_path(&out_csv).map_err(|e| format!("Failed to create CSV writer: {:?}", e))?;

    // (A) Construct the header row
    // First column: "method"
    let mut header: Vec<String> = vec!["method".to_string()];

    // For each ds, query, we'll produce 2 columns: ds_qX_mse, ds_qX_max_err
    // Must do it in a consistent order so each row lines up
    let mut ds_query_pairs = Vec::new();
    for ds in &data_sources {
        for &q in &queries {
            ds_query_pairs.push((ds.to_string(), q));
            header.push(format!("{}_q{}_mse", ds, q));
            header.push(format!("{}_q{}_max_err", ds, q));
        }
    }

    // Write the header row
    wtr.write_record(&header)
        .map_err(|e| format!("Failed to write header: {:?}", e))?;

    // (B) For each method, produce exactly one CSV row
    for method in &methods {
        let method_str = method.to_string();

        // Collect row data:
        // row[0] = method, row[1..] = MSE/Max-Err in the same ds_query order
        let mut row = Vec::new();
        row.push(method_str.clone());

        // Then for each (ds, q), find the result in the map
        for (ds_s, q_s) in &ds_query_pairs {
            let key = (method_str.clone(), ds_s.clone(), *q_s);
            if let Some(m) = results_map.get(&key) {
                row.push(format!("{}", m.mse));
                row.push(format!("{}", m.max_abs));
            } else {
                // If missing, put e.g. "NA"
                row.push("NA".to_string());
                row.push("NA".to_string());
            }
        }

        // Write the row
        wtr.write_record(&row)
            .map_err(|e| format!("Failed to write row: {:?}", e))?;
    }

    // (C) Flush
    wtr.flush()
        .map_err(|e| format!("Failed to flush CSV: {:?}", e))?;

    println!("Wrote 'wide' CSV results to {}", out_csv);
    Ok(())
}

/// Same as before: runs the query & writes out qX_<method>_eval.json, etc.
fn run_quantile_generation(
    memory_size: usize,
    bp: Arc<BufferPool>,
    query_id: u32,
    methods: &[QuantileMethod],
    num_quantiles: usize,
    data_source: &str,
    base_path: &str,
) -> Result<(), String> {
    let storage = Arc::new(OnDiskStorage::<BufferPool>::load(&bp));
    let (db_id, catalog) =
        load_db(&storage, data_source).map_err(|e| format!("Failed to load DB: {:?}", e))?;

    // read the .sql
    let prefix = if let Some(prefix) = data_source.split("-").next() {
        prefix.to_string()
    } else {
        return Err("Failed to extract keyword".to_string());
    };
    let query_path = format!("{}/queries/q{}.sql", prefix, query_id);
    let sql_string = fs::read_to_string(&query_path)
        .map_err(|e| format!("Failed to read SQL file {}: {}", query_path, e))?;

    let logical = to_logical(db_id, &catalog, &sql_string)
        .map_err(|e| format!("Failed to convert to logical: {:?}", e))?;
    let physical = to_physical(logical);

    let mem_policy = Arc::new(MemoryPolicy::FixedSizeLimit(memory_size));
    let temp_c_id = 1000;
    let exclude_last_pipeline = true;
    let exe = OnDiskPipelineGraph::new(
        db_id,
        temp_c_id as ContainerId,
        &catalog,
        &storage,
        &bp,
        &mem_policy,
        physical,
        exclude_last_pipeline,
    );

    // Filenames
    // let method_str = method.to_string();
    let estimated_path = format!("{}/q{}_***_estimated.json", base_path, query_id);
    let actual_path = format!("{}/q{}_actual.json", base_path, query_id);
    let eval_path = format!("{}/q{}_***_eval.json", base_path, query_id);

    let _ = quantile_generation_execute(
        db_id,
        &storage,
        exe,
        false,
        data_source,
        query_id as u8,
        methods,
        num_quantiles,
        &estimated_path,
        &actual_path,
        &eval_path,
    );
    Ok(())
}

/// Parse the `qX_method_eval.json` to extract `(mse, max_abs_error)`
fn parse_evaluation_json(eval_path: &str) -> Result<(f64, f64), String> {
    #[derive(Deserialize)]
    struct EvalRecord {
        mse: f64,
        max_abs_error: f64,
        // ...
    }

    let content = fs::read_to_string(eval_path)
        .map_err(|e| format!("Can't read eval file '{}': {:?}", eval_path, e))?;
    let er: EvalRecord = serde_json::from_str(&content)
        .map_err(|e| format!("Can't parse JSON in '{}': {:?}", eval_path, e))?;

    Ok((er.mse, er.max_abs_error))
}
