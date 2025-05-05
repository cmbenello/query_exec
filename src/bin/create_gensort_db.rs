use clap::Parser;
use fbtree::container::ContainerManager;
use query_exec::{
    prelude::{
        create_db, create_table_from_sql, import_gensort, Catalog, ContainerDS, OnDiskStorage,
    },
    BufferPool,
};
use std::{path::PathBuf, sync::Arc};

#[derive(Debug, Parser)]
#[clap(name = "gensort", about = "Loads Gensort binary data into a new DB.")]
pub struct GensortOpt {
    /// Scale factor
    #[clap(short = 's', long = "scale-factor", default_value = "1.0")]
    pub scale_factor: f64,

    /// Buffer pool size
    #[clap(short = 'b', long = "buffer-pool-size", default_value = "100000")]
    pub buffer_pool_size: usize,

    /// A custom name appended to the directory, e.g. "uniform"
    #[clap(short = 'n', long = "name", default_value = "uniform")]
    pub name: String,
}

/// Returns a brand-new empty catalog.
fn get_catalog() -> Arc<Catalog> {
    Arc::new(Catalog::new())
}

/// Creates a BufferPool in the given directory, with the specified # of frames.
fn get_bp(dir: &str, num_frames: usize) -> Arc<BufferPool> {
    let cm = Arc::new(ContainerManager::new(&dir, true, false).unwrap());
    let bp = Arc::new(BufferPool::new(num_frames, cm).unwrap());
    bp
}

/// Creates a new DB "GENSORT", loads the CREATE TABLE statement from
/// `gensort_data.sql`, then imports raw Gensort records from
/// `gensort/data/sf-{sf}-{name}` using `import_gensort`.
pub fn create_gensort_db(sf: f64, buffer_pool_size: usize, name: &str) {
    // 1. Check for the input file path: "gensort/data/sf-<sf>-<name>"
    let data_path = format!("gensort/{}", name);
    if !PathBuf::from(&data_path).exists() {
        panic!(
            "Data file {} does not exist. Please provide valid Gensort data.",
            data_path
        );
    }
    println!("Loading Gensort binary data from {}", data_path);

    // 2. Build a unique directory for the buffer pool, e.g. "bp-dir-gensort-sf-1.0-uniform"
    let bp_dir = format!("bp-dir-gensort-sf-{}-{}", sf, name);
    if PathBuf::from(&bp_dir).exists() {
        panic!("Buffer pool directory {} already exists.", bp_dir);
    }
    // 3. Create the catalog, buffer pool, and OnDiskStorage
    let catalog = get_catalog();
    let bp = get_bp(&bp_dir, buffer_pool_size);
    let storage = Arc::new(OnDiskStorage::new(&bp));
    // 4. Create a database named "GENSORT"
    let db_id = create_db(&storage, "GENSORT").expect("Failed to create DB 'GENSORT'");

    // 5. Read the CREATE TABLE statement from "gensort/tables/gensort_data.sql"
    let table_sql_path = "gensort/tables/gensort_data.sql";
    let table_sql =
        std::fs::read_to_string(table_sql_path).expect("Failed to read gensort_data.sql file.");
    // 6. Create the table in "GENSORT" using your existing function
    let c_id = create_table_from_sql(&catalog, &storage, db_id, &table_sql, ContainerDS::Gensort)
        .expect("Failed to create gensort_data table");
    // 7. Import raw gensort records from data_path using `import_gensort`
    import_gensort(&catalog, &storage, db_id, c_id, &data_path)
        .expect("Error importing Gensort data");

    println!("Successfully loaded Gensort data into DB 'GENSORT' using table definition from gensort_data.sql.");
    println!("Buffer pool files are in {}", bp_dir);
}

/// Example `main` using Clap:
fn main() {
    let opt = GensortOpt::parse();
    create_gensort_db(opt.scale_factor, opt.buffer_pool_size, &opt.name);
}
