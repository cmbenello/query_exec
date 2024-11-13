use std::path::Path;
use std::sync::Arc;

use fbtree::prelude::{
    ContainerId, ContainerOptions, ContainerType, DatabaseId, TxnStorageStatus, TxnStorageTrait,
};
use fbtree::txn_storage::{DBOptions, TxnOptions};
use sqlparser::parser::ParserError;
use sqlparser::{dialect::GenericDialect, parser::Parser};

use crate::catalog::CatalogRef;
use crate::error::ExecError;
use crate::executor::{Executor, TupleBuffer, TupleBufferIter};
use crate::expression::prelude::{
    HeuristicRules, LogicalRelExpr, LogicalToPhysicalRelExpr, PhysicalRelExpr,
};
use crate::loader::prelude::SimpleCsvLoader;
use crate::loader::DataLoader;
use crate::parser::{Translator, TranslatorError};
use crate::prelude::{Catalog, ColumnDef, DataType, Schema, SchemaRef, Table};

pub fn print_tuples(tuples: Arc<impl TupleBuffer>) {
    let mut count = 0;
    let mut tuples = tuples.iter();
    while let Some(t) = tuples.next().unwrap() {
        count += 1;
        println!("{}", t.to_pretty_string());
    }
    println!("Total tuples: {}", count);
}

#[derive(Debug)]
pub enum QueryExecutorError {
    InvalidCSV(String),
    InvalidTable(String),
    InvalidSqlString(ParserError),
    InvalidAst(TranslatorError),
    InvalidLogicalPlan,
    InvalidPhysicalPlan,
    InvalidExecutable,
    ExecutionError(ExecError),
}

impl From<ParserError> for QueryExecutorError {
    fn from(error: ParserError) -> Self {
        QueryExecutorError::InvalidSqlString(error)
    }
}

impl From<TranslatorError> for QueryExecutorError {
    fn from(error: TranslatorError) -> Self {
        QueryExecutorError::InvalidAst(error)
    }
}

impl From<ExecError> for QueryExecutorError {
    fn from(error: ExecError) -> Self {
        QueryExecutorError::ExecutionError(error)
    }
}

impl From<TxnStorageStatus> for QueryExecutorError {
    fn from(status: TxnStorageStatus) -> Self {
        QueryExecutorError::ExecutionError(ExecError::Storage(status.into()))
    }
}

pub fn parse_sql(sql: &str) -> Result<sqlparser::ast::Query, QueryExecutorError> {
    let dialect = GenericDialect {};
    let statements = Parser::new(&dialect)
        .try_with_sql(sql)?
        .parse_statements()?;
    let query = {
        let statement = statements.into_iter().next().unwrap();
        match statement {
            sqlparser::ast::Statement::Query(query) => query,
            other => {
                return Err(QueryExecutorError::InvalidSqlString(
                    ParserError::ParserError(format!("Expected a query, got {:?}", other)),
                ))
            }
        }
    };
    Ok(*query)
}

pub fn to_logical(
    db_id: DatabaseId,
    catalog_ref: &CatalogRef,
    sql_string: &str,
) -> Result<LogicalRelExpr, QueryExecutorError> {
    let query = parse_sql(sql_string)?;
    let logical_rules = Arc::new(HeuristicRules::default());
    let mut translator = Translator::new(db_id, catalog_ref, &logical_rules);
    let result = translator.process_query(&query)?;
    Ok(result.plan)
}

pub fn to_physical(logical_plan: LogicalRelExpr) -> PhysicalRelExpr {
    LogicalToPhysicalRelExpr.to_physical(logical_plan)
}

pub fn execute<T: TxnStorageTrait, E: Executor<T>>(
    db_id: DatabaseId,
    storage: &Arc<T>,
    exec: E,
    verbose: bool,
) -> Arc<impl TupleBuffer> {
    if verbose {
        println!("=== Executor ===");
        println!("{}", exec.to_pretty_string());
    }
    let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
    let result = exec.execute(&txn).unwrap();
    storage.commit_txn(&txn, false).unwrap();
    result
}

fn parse_create_table(sql: &str) -> Result<(String, SchemaRef), QueryExecutorError> {
    let dialect = GenericDialect {};
    let statements = Parser::new(&dialect)
        .try_with_sql(sql)?
        .parse_statements()?;
    let statement = statements.into_iter().next().unwrap();
    match statement {
        sqlparser::ast::Statement::CreateTable {
            name,
            columns,
            constraints,
            ..
        } => {
            // Create a schema
            let col_defs = columns
                .iter()
                .map(|c| {
                    let data_type = match &c.data_type {
                        sqlparser::ast::DataType::Int(_) | sqlparser::ast::DataType::Integer(_) => {
                            DataType::Int
                        }
                        sqlparser::ast::DataType::Text => DataType::String,
                        sqlparser::ast::DataType::Boolean => DataType::Boolean,
                        sqlparser::ast::DataType::Float(_)
                        | sqlparser::ast::DataType::Double
                        | sqlparser::ast::DataType::Decimal(_) => DataType::Float,
                        sqlparser::ast::DataType::Char(_) => DataType::String,
                        sqlparser::ast::DataType::Varchar(_) => DataType::String,
                        sqlparser::ast::DataType::Date => DataType::Date,
                        other => {
                            return Err(QueryExecutorError::InvalidSqlString(
                                ParserError::ParserError(format!(
                                    "Unsupported data type: {:?}",
                                    other
                                )),
                            ))
                        }
                    };
                    let not_null = c
                        .options
                        .iter()
                        .any(|o| matches!(o.option, sqlparser::ast::ColumnOption::NotNull));
                    Ok(ColumnDef::new(&c.name.value, data_type, !not_null))
                })
                .collect::<Result<Vec<_>, _>>()?;

            // Find the column index of the primary key.
            let inline_pk = columns
                .iter()
                .enumerate()
                .filter_map(|(i, c)| {
                    c.options
                        .iter()
                        .find(|o| {
                            matches!(
                                o.option,
                                sqlparser::ast::ColumnOption::Unique {
                                    is_primary: true,
                                    ..
                                }
                            )
                        })
                        .map(|_| i)
                })
                .collect::<Vec<_>>();
            let external_pk = constraints.iter().find_map(|c| match c {
                sqlparser::ast::TableConstraint::PrimaryKey { columns: pks, .. } => Some(
                    pks.iter()
                        .map(|pk| {
                            columns
                                .iter()
                                .position(|c| c.name.value == pk.value)
                                .unwrap()
                        })
                        .collect::<Vec<_>>(),
                ),
                _ => None,
            });
            let primary_key = match (inline_pk.len(), external_pk) {
                (0, None) => {
                    return Err(QueryExecutorError::InvalidSqlString(
                        ParserError::ParserError("Primary key not found".to_string()),
                    ));
                }
                (1, None) => inline_pk,
                (0, Some(pk)) => pk,
                _ => {
                    return Err(QueryExecutorError::InvalidSqlString(
                        ParserError::ParserError(
                            "Multiple primary keys are not supported".to_string(),
                        ),
                    ))
                }
            };
            let schema = Arc::new(Schema::new(col_defs, primary_key));
            // name joined with a .
            let table_name = name.to_string();
            Ok((table_name, schema))
        }
        other => Err(QueryExecutorError::InvalidSqlString(
            ParserError::ParserError(format!(
                "Expected a CREATE TABLE statement, got {:?}",
                other
            )),
        )),
    }
}

/// Create a database
/// This will create a container with container_id 0 for the catalog.
pub fn create_db<T: TxnStorageTrait>(
    storage: &Arc<T>,
    db_name: &str,
) -> Result<DatabaseId, QueryExecutorError> {
    let db_id = storage.open_db(DBOptions::new(db_name))?;
    // Create a container for the catalog
    let txn = storage.begin_txn(&db_id, Default::default())?;
    let c_id = storage
        .create_container(
            &txn,
            &db_id,
            ContainerOptions::new("catalog", ContainerType::BTree),
        )
        .unwrap();
    assert_eq!(c_id, 0);
    storage.commit_txn(&txn, false)?;
    Ok(db_id)
}

/// Load a database from storage
/// The catalog is loaded into memory.
/// Catalog has a mapping from container_id to Table, which is created at container_id 0.
pub fn load_db<T: TxnStorageTrait>(
    storage: &Arc<T>,
    db_name: &str,
) -> Result<(DatabaseId, CatalogRef), QueryExecutorError> {
    let db_id = storage.open_db(DBOptions::new(db_name))?;
    // Scan container_id 0 to load the catalog
    let txn = storage.begin_txn(&db_id, Default::default())?;
    let catalog = Arc::new(Catalog::new());
    let catalog_c_id = 0; // catalog container_id is 0
    let iter = storage.scan_range(&txn, &catalog_c_id, Default::default())?;
    while let Some((k, v)) = storage.iter_next(&iter)? {
        let c_id = ContainerId::from_be_bytes(k.try_into().unwrap());
        let table = Table::from_bytes(&v);
        catalog.add_table(c_id, Arc::new(table));
    }
    storage.commit_txn(&txn, false)?;
    Ok((db_id, catalog))
}

pub fn create_table_from_sql<T: TxnStorageTrait>(
    catalog_ref: &CatalogRef,
    storage: &Arc<T>,
    db_id: DatabaseId,
    sql_string: &str,
    container_type: ContainerType,
) -> Result<ContainerId, QueryExecutorError> {
    let (table_name, schema) = parse_create_table(sql_string)?;
    create_table(
        catalog_ref,
        storage,
        db_id,
        &table_name,
        schema,
        container_type,
    )
}

pub fn create_table<T: TxnStorageTrait>(
    catalog_ref: &CatalogRef,
    storage: &Arc<T>,
    db_id: DatabaseId,
    table_name: &str,
    schema: SchemaRef,
    container_type: ContainerType,
) -> Result<ContainerId, QueryExecutorError> {
    let txn = storage.begin_txn(&db_id, Default::default())?;
    let c_id = storage.create_container(
        &txn,
        &db_id,
        ContainerOptions::new(table_name, container_type),
    )?;
    // Insert the catalog entry into the catalog container
    let catalog_c_id = 0;
    let table = Table::new(table_name, schema.clone());
    storage.insert_value(
        &txn,
        &catalog_c_id,
        c_id.to_be_bytes().to_vec(),
        table.to_bytes(),
    )?;
    catalog_ref.add_table(c_id, Arc::new(Table::new(table_name, schema)));

    storage.commit_txn(&txn, false)?;
    Ok(c_id)
}

pub fn import_csv<P: AsRef<Path>, T: TxnStorageTrait>(
    catalog_ref: &CatalogRef,
    storage: &Arc<T>,
    db_id: DatabaseId,
    c_id: ContainerId,
    csv_path: P,
    has_header: bool,
    delimiter: u8,
) -> Result<(), QueryExecutorError> {
    let rdr = csv::ReaderBuilder::new()
        .has_headers(has_header)
        .delimiter(delimiter)
        .from_path(csv_path.as_ref())
        .map_err(|e| QueryExecutorError::InvalidCSV(e.to_string()))?;
    let mut loader = SimpleCsvLoader::new(rdr, storage.clone());
    loader
        .load_data(
            catalog_ref
                .get_schema(c_id)
                .ok_or(QueryExecutorError::InvalidTable(
                    "Table not found".to_string(),
                ))?,
            db_id,
            c_id,
        )
        .map_err(|e| QueryExecutorError::InvalidCSV(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use fbtree::{
        bp::{get_test_bp, BufferPool},
        prelude::{ContainerId, ContainerType, InMemStorage, TxnOptions},
        random::gen_random_pathname,
        txn_storage::OnDiskStorage,
    };

    use crate::{
        catalog::Catalog,
        executor::prelude::{
            InMemPipelineGraph, MemoryPolicy, OnDiskPipelineGraph, VolcanoIterator,
        },
        tuple::Tuple,
        Field,
    };

    use super::*;

    fn get_in_mem_storage() -> Arc<InMemStorage> {
        Arc::new(InMemStorage::new())
    }

    fn setup_employees_table<T: TxnStorageTrait>(
        storage: &Arc<T>,
        db_id: DatabaseId,
        catalog: &CatalogRef,
    ) -> ContainerId {
        // Create Employees table
        // Schema: id, name, age, department_id
        // 5 tuples

        // Create Employees table
        // Schema: id, name, age, department_id
        // 5 tuples
        /*
        id,name,age,department_id
        1,Alice,30,1
        2,Bob,22,2
        3,Charlie,35,1
        4,David,28,2
        5,Eva,40,NULL
         */
        let c_id = create_table_from_sql(
            catalog,
            storage,
            db_id,
            "CREATE TABLE Employees (id INT, name VARCHAR, age INT, department_id INT, PRIMARY KEY (id))",
            ContainerType::BTree,
        ).unwrap();
        let schema = catalog.get_schema(c_id).unwrap();

        let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
        let data = vec![
            Tuple::from_fields(vec![1.into(), "Alice".into(), 30.into(), 1.into()]),
            Tuple::from_fields(vec![2.into(), "Bob".into(), 22.into(), 2.into()]),
            Tuple::from_fields(vec![3.into(), "Charlie".into(), 35.into(), 1.into()]),
            Tuple::from_fields(vec![4.into(), "David".into(), 28.into(), 2.into()]),
            Tuple::from_fields(vec![5.into(), "Eva".into(), 40.into(), Field::Int(None)]),
        ];

        storage
            .insert_values(
                &txn,
                &c_id,
                data.into_iter()
                    .map(|t| {
                        (
                            t.to_primary_key_bytes(schema.primary_key_indices()),
                            t.to_bytes(),
                        )
                    })
                    .collect(),
            )
            .unwrap();

        storage.commit_txn(&txn, false).unwrap();

        c_id
    }

    fn setup_departments_table<T: TxnStorageTrait>(
        storage: &Arc<T>,
        db_id: DatabaseId,
        catalog: &CatalogRef,
    ) -> ContainerId {
        // Create Departments table
        // Schema: id, name
        // 3 tuples
        let c_id = create_table_from_sql(
            catalog,
            storage,
            db_id,
            "CREATE TABLE Departments (id INT, name VARCHAR, PRIMARY KEY (id))",
            ContainerType::BTree,
        )
        .unwrap();
        let schema = catalog.get_schema(c_id).unwrap();

        /*
        id,name
        1,HR
        2,Engineering
        3,Marketing
        */

        let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
        let data = vec![
            Tuple::from_fields(vec![1.into(), "HR".into()]),
            Tuple::from_fields(vec![2.into(), "Engineering".into()]),
            Tuple::from_fields(vec![3.into(), "Marketing".into()]),
        ];

        storage
            .insert_values(
                &txn,
                &c_id,
                data.into_iter()
                    .map(|t| {
                        (
                            t.to_primary_key_bytes(schema.primary_key_indices()),
                            t.to_bytes(),
                        )
                    })
                    .collect(),
            )
            .unwrap();

        storage.commit_txn(&txn, false).unwrap();

        c_id
    }

    #[test]
    fn test_catalog_durability() {
        let tempdir = tempfile::tempdir().unwrap();
        let db_name = gen_random_pathname(Some("test_db"));

        let (db_id, emp_id, dep_id, original_catalog) = {
            let bp = Arc::new(BufferPool::new(&tempdir, 10, false).unwrap());
            let storage = Arc::new(OnDiskStorage::new(&bp));
            let db_id = create_db(&storage, &db_name).unwrap();

            let catalog = Catalog::new();
            let catalog_ref = catalog.into();
            let c_id = setup_employees_table(&storage, db_id, &catalog_ref);
            let d_id = setup_departments_table(&storage, db_id, &catalog_ref);
            (db_id, c_id, d_id, catalog_ref)
        };

        let bp = Arc::new(BufferPool::new(&tempdir, 10, false).unwrap());
        let storage = Arc::new(OnDiskStorage::load(&bp));
        let (new_db_id, new_catalog) = load_db(&storage, &db_name).unwrap();
        assert_eq!(db_id, new_db_id);
        assert_eq!(
            original_catalog.get_schema(emp_id).unwrap(),
            new_catalog.get_schema(emp_id).unwrap()
        );
        assert_eq!(
            original_catalog.get_schema(dep_id).unwrap(),
            new_catalog.get_schema(dep_id).unwrap()
        );
    }

    fn check_result(
        result: Arc<impl TupleBuffer>,
        expected: &mut [Tuple],
        sorted: bool,
        verbose: bool,
    ) {
        let mut vec = Vec::new();
        let mut result = result.iter();
        while let Some(t) = result.next().unwrap() {
            vec.push(t);
        }
        let mut result = vec;
        if sorted {
            result.sort();
            expected.sort();
        }
        let result_string = result
            .iter()
            .map(|t| t.to_pretty_string())
            .collect::<Vec<_>>()
            .join("\n");
        let expected_string = expected
            .iter()
            .map(|t: &Tuple| t.to_pretty_string())
            .collect::<Vec<_>>()
            .join("\n");
        if verbose {
            println!("--- Result ---\n{}", result_string);
            println!("--- Expected ---\n{}", expected_string);
        }
        assert_eq!(
            result, expected,
            "\n--- Result ---\n{}\n--- Expected ---\n{}\n",
            result_string, expected_string
        );
    }

    fn get_physical_plan(
        db_id: DatabaseId,
        catalog_ref: &CatalogRef,
        sql_string: &str,
        verbose: bool,
    ) -> PhysicalRelExpr {
        let logical_plan = to_logical(db_id, catalog_ref, sql_string).unwrap();
        if verbose {
            println!("=== Logical Plan ===");
            logical_plan.pretty_print();
        }
        let physical_plan = to_physical(logical_plan);
        if verbose {
            println!("=== Physical Plan ===");
            physical_plan.pretty_print();
        }
        physical_plan
    }

    fn volcano_executor(sql: &str) -> Arc<impl TupleBuffer> {
        let storage = get_in_mem_storage();
        let db_id = create_db(&storage, "test").unwrap();
        let catalog = Catalog::new();
        let catalog_ref = catalog.into();
        let _ = setup_employees_table(&storage, db_id, &catalog_ref);
        let _ = setup_departments_table(&storage, db_id, &catalog_ref);
        let physical_plan = get_physical_plan(db_id, &catalog_ref, sql, true);
        let exe = VolcanoIterator::new(&catalog_ref, &storage, physical_plan);
        execute(db_id, &storage, exe, true)
    }

    fn inmem_pipeline_executor(sql: &str) -> Arc<impl TupleBuffer> {
        let storage = get_in_mem_storage();
        let db_id = create_db(&storage, "test").unwrap();
        let catalog = Catalog::new();
        let catalog_ref = catalog.into();
        let _ = setup_employees_table(&storage, db_id, &catalog_ref);
        let _ = setup_departments_table(&storage, db_id, &catalog_ref);
        let physical_plan = get_physical_plan(db_id, &catalog_ref, sql, true);
        let exe = InMemPipelineGraph::new(&catalog_ref, &storage, physical_plan);
        execute(db_id, &storage, exe, true)
    }

    fn ondisk_pipeline_executor(sql: &str) -> Arc<impl TupleBuffer> {
        let mem_pool = get_test_bp(1024);
        let storage = Arc::new(OnDiskStorage::new(&mem_pool));
        let db_id = create_db(&storage, "test").unwrap();
        let catalog = Catalog::new();
        let catalog_ref = catalog.into();
        let _ = setup_employees_table(&storage, db_id, &catalog_ref);
        let _ = setup_departments_table(&storage, db_id, &catalog_ref);
        let physical_plan = get_physical_plan(db_id, &catalog_ref, sql, true);
        let mem_policy = Arc::new(MemoryPolicy::FixedSizeLimit(10));
        let temp_c_id = 1000;
        let exe = OnDiskPipelineGraph::new(
            db_id,
            temp_c_id,
            &catalog_ref,
            &storage,
            &mem_pool,
            &mem_policy,
            physical_plan,
            false,
        );
        execute(db_id, &storage, exe, true)
    }

    use rstest::rstest;

    #[rstest]
    #[case::volcano(volcano_executor)]
    #[case::inmem_pipeline(inmem_pipeline_executor)]
    #[case::ondisk_pipeline(ondisk_pipeline_executor)]
    fn test_projection<B: TupleBuffer>(#[case] exec: impl Fn(&str) -> Arc<B>) {
        let result = exec("SELECT name, age FROM Employees");
        let mut expected = vec![
            Tuple::from_fields(vec!["Alice".into(), 30.into()]),
            Tuple::from_fields(vec!["Bob".into(), 22.into()]),
            Tuple::from_fields(vec!["Charlie".into(), 35.into()]),
            Tuple::from_fields(vec!["David".into(), 28.into()]),
            Tuple::from_fields(vec!["Eva".into(), 40.into()]),
        ];
        check_result(result, &mut expected, true, false);
    }

    #[rstest]
    #[case::volcano(volcano_executor)]
    #[case::inmem_pipeline(inmem_pipeline_executor)]
    #[case::ondisk_pipeline(ondisk_pipeline_executor)]
    fn test_projection_with_expression<B: TupleBuffer>(#[case] exec: impl Fn(&str) -> Arc<B>) {
        let result = exec("SELECT name, age + 1 FROM Employees");
        let mut expected = vec![
            Tuple::from_fields(vec!["Alice".into(), 31.into()]),
            Tuple::from_fields(vec!["Bob".into(), 23.into()]),
            Tuple::from_fields(vec!["Charlie".into(), 36.into()]),
            Tuple::from_fields(vec!["David".into(), 29.into()]),
            Tuple::from_fields(vec!["Eva".into(), 41.into()]),
        ];
        check_result(result, &mut expected, true, false);
    }

    #[rstest]
    #[case::volcano(volcano_executor)]
    #[case::inmem_pipeline(inmem_pipeline_executor)]
    #[case::ondisk_pipeline(ondisk_pipeline_executor)]
    fn test_filter<B: TupleBuffer>(#[case] exec: impl Fn(&str) -> Arc<B>) {
        let sql_string = "SELECT name, age FROM Employees WHERE age > 30";
        let result = exec(sql_string);
        let mut expected = vec![
            Tuple::from_fields(vec!["Charlie".into(), 35.into()]),
            Tuple::from_fields(vec!["Eva".into(), 40.into()]),
        ];
        check_result(result, &mut expected, true, false);
    }

    #[rstest]
    #[case::volcano(volcano_executor)]
    #[case::inmem_pipeline(inmem_pipeline_executor)]
    #[case::ondisk_pipeline(ondisk_pipeline_executor)]
    fn test_join<B: TupleBuffer>(#[case] exec: impl Fn(&str) -> Arc<B>) {
        let sql_string = "SELECT e.name, e.age, d.name AS department FROM Employees e JOIN Departments d ON e.department_id = d.id";
        let result = exec(sql_string);
        let mut expected = vec![
            Tuple::from_fields(vec!["Alice".into(), 30.into(), "HR".into()]),
            Tuple::from_fields(vec!["Bob".into(), 22.into(), "Engineering".into()]),
            Tuple::from_fields(vec!["Charlie".into(), 35.into(), "HR".into()]),
            Tuple::from_fields(vec!["David".into(), 28.into(), "Engineering".into()]),
        ];
        check_result(result, &mut expected, true, true);
    }

    #[rstest]
    #[case::volcano(volcano_executor)]
    #[case::inmem_pipeline(inmem_pipeline_executor)]
    #[case::ondisk_pipeline(ondisk_pipeline_executor)]
    fn test_left_outer_join<B: TupleBuffer>(#[case] exec: impl Fn(&str) -> Arc<B>) {
        let sql_string = "SELECT d.name, sum(e.age) FROM Departments d LEFT OUTER JOIN Employees e ON e.department_id = d.id GROUP BY d.name";
        let result = exec(sql_string);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into(), 65.into()]),
            Tuple::from_fields(vec!["Engineering".into(), 50.into()]),
            Tuple::from_fields(vec!["Marketing".into(), Field::Int(None)]),
        ];
        check_result(result, &mut expected, true, true);
    }

    #[rstest]
    #[case::volcano(volcano_executor)]
    #[case::inmem_pipeline(inmem_pipeline_executor)]
    #[case::ondisk_pipeline(ondisk_pipeline_executor)]
    fn test_right_outer_join<B: TupleBuffer>(#[case] exec: impl Fn(&str) -> Arc<B>) {
        let sql_string =  "SELECT d.name, sum(e.age) FROM Employees e RIGHT OUTER JOIN Departments d ON e.department_id = d.id GROUP BY d.name";
        let result = exec(sql_string);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into(), 65.into()]),
            Tuple::from_fields(vec!["Engineering".into(), 50.into()]),
            Tuple::from_fields(vec!["Marketing".into(), Field::Int(None)]),
        ];
        check_result(result, &mut expected, true, true);
    }

    #[rstest]
    #[case::volcano(volcano_executor)]
    #[case::inmem_pipeline(inmem_pipeline_executor)]
    #[case::ondisk_pipeline(ondisk_pipeline_executor)]
    fn test_left_semi_join<B: TupleBuffer>(#[case] exec: impl Fn(&str) -> Arc<B>) {
        let sql_string =
            "SELECT d.name FROM Departments d LEFT SEMI JOIN Employees e ON e.department_id = d.id";
        let result = exec(sql_string);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into()]),
            Tuple::from_fields(vec!["Engineering".into()]),
        ];
        check_result(result, &mut expected, true, true);
    }

    #[rstest]
    #[case::volcano(volcano_executor)]
    #[case::inmem_pipeline(inmem_pipeline_executor)]
    #[case::ondisk_pipeline(ondisk_pipeline_executor)]
    fn test_right_semi_join<B: TupleBuffer>(#[case] exec: impl Fn(&str) -> Arc<B>) {
        let sql_string = "SELECT d.name FROM Employees e RIGHT SEMI JOIN Departments d ON e.department_id = d.id";
        let result = exec(sql_string);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into()]),
            Tuple::from_fields(vec!["Engineering".into()]),
        ];
        check_result(result, &mut expected, true, true);
    }

    #[rstest]
    #[case::volcano(volcano_executor)]
    #[case::inmem_pipeline(inmem_pipeline_executor)]
    #[case::ondisk_pipeline(ondisk_pipeline_executor)]
    fn test_left_anti_join<B: TupleBuffer>(#[case] exec: impl Fn(&str) -> Arc<B>) {
        let sql_string =
            "SELECT d.name FROM Departments d LEFT ANTI JOIN Employees e ON e.department_id = d.id";
        let result = exec(sql_string);
        let mut expected = vec![Tuple::from_fields(vec!["Marketing".into()])];
        check_result(result, &mut expected, true, true);
    }

    #[rstest]
    #[case::volcano(volcano_executor)]
    #[case::inmem_pipeline(inmem_pipeline_executor)]
    #[case::ondisk_pipeline(ondisk_pipeline_executor)]
    fn test_right_anti_join<B: TupleBuffer>(#[case] exec: impl Fn(&str) -> Arc<B>) {
        let sql_string = "SELECT d.name FROM Employees e RIGHT ANTI JOIN Departments d ON e.department_id = d.id";
        let result = exec(sql_string);
        let mut expected = vec![Tuple::from_fields(vec!["Marketing".into()])];
        check_result(result, &mut expected, true, true);
    }

    #[rstest]
    #[case::volcano(volcano_executor)]
    #[case::inmem_pipeline(inmem_pipeline_executor)]
    fn test_cross_join<B: TupleBuffer>(#[case] exec: impl Fn(&str) -> Arc<B>) {
        let sql_string =
            "SELECT e.name, e.age, d.name AS department FROM Employees e, Departments d";
        let result = exec(sql_string);
        let mut expected = vec![
            Tuple::from_fields(vec!["Alice".into(), 30.into(), "HR".into()]),
            Tuple::from_fields(vec!["Alice".into(), 30.into(), "Engineering".into()]),
            Tuple::from_fields(vec!["Alice".into(), 30.into(), "Marketing".into()]),
            Tuple::from_fields(vec!["Bob".into(), 22.into(), "HR".into()]),
            Tuple::from_fields(vec!["Bob".into(), 22.into(), "Engineering".into()]),
            Tuple::from_fields(vec!["Bob".into(), 22.into(), "Marketing".into()]),
            Tuple::from_fields(vec!["Charlie".into(), 35.into(), "HR".into()]),
            Tuple::from_fields(vec!["Charlie".into(), 35.into(), "Engineering".into()]),
            Tuple::from_fields(vec!["Charlie".into(), 35.into(), "Marketing".into()]),
            Tuple::from_fields(vec!["David".into(), 28.into(), "HR".into()]),
            Tuple::from_fields(vec!["David".into(), 28.into(), "Engineering".into()]),
            Tuple::from_fields(vec!["David".into(), 28.into(), "Marketing".into()]),
            Tuple::from_fields(vec!["Eva".into(), 40.into(), "HR".into()]),
            Tuple::from_fields(vec!["Eva".into(), 40.into(), "Engineering".into()]),
            Tuple::from_fields(vec!["Eva".into(), 40.into(), "Marketing".into()]),
        ];
        check_result(result, &mut expected, true, false);
    }

    #[rstest]
    #[case::volcano(volcano_executor)]
    #[case::inmem_pipeline(inmem_pipeline_executor)]
    #[case::ondisk_pipeline(ondisk_pipeline_executor)]
    fn test_aggregate<B: TupleBuffer>(#[case] exec: impl Fn(&str) -> Arc<B>) {
        let sql_string = "SELECT COUNT(*), AVG(age) FROM Employees";
        let result = exec(sql_string);
        let mut expected = vec![Tuple::from_fields(vec![5.into(), 31.0.into()])];
        check_result(result, &mut expected, true, false);
    }

    #[rstest]
    #[case::volcano(volcano_executor)]
    #[case::inmem_pipeline(inmem_pipeline_executor)]
    #[case::ondisk_pipeline(ondisk_pipeline_executor)]
    fn test_groupby_aggregate<B: TupleBuffer>(#[case] exec: impl Fn(&str) -> Arc<B>) {
        let sql_string = "SELECT d.name, AVG(e.age) AS average_age FROM Employees e JOIN Departments d ON e.department_id = d.id GROUP BY d.name;";
        let result = exec(sql_string);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into(), 32.5.into()]),
            Tuple::from_fields(vec!["Engineering".into(), 25.0.into()]),
        ];
        check_result(result, &mut expected, true, false);
    }

    #[rstest]
    #[case::volcano(volcano_executor)]
    #[case::inmem_pipeline(inmem_pipeline_executor)]
    #[case::ondisk_pipeline(ondisk_pipeline_executor)]
    fn test_groupby_aggregate_having<B: TupleBuffer>(#[case] exec: impl Fn(&str) -> Arc<B>) {
        let sql_string = "SELECT d.name, AVG(e.age) AS average_age FROM Employees e JOIN Departments d ON e.department_id = d.id GROUP BY d.name HAVING AVG(e.age) > 30";
        let result = exec(sql_string);
        let mut expected = vec![Tuple::from_fields(vec!["HR".into(), 32.5.into()])];
        check_result(result, &mut expected, true, false);
    }

    #[rstest]
    #[case::volcano(volcano_executor)]
    #[case::inmem_pipeline(inmem_pipeline_executor)]
    #[case::ondisk_pipeline(ondisk_pipeline_executor)]
    fn test_subquery<B: TupleBuffer>(#[case] exec: impl Fn(&str) -> Arc<B>) {
        // For each department, count the number of employees and sum of their ages
        let sql_string = "SELECT d.name, cnt, sum_age FROM Departments d, (SELECT department_id, COUNT(*) AS cnt, SUM(age) AS sum_age FROM Employees e WHERE e.department_id = d.id)";
        let result = exec(sql_string);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into(), 2.into(), 65.into()]),
            Tuple::from_fields(vec!["Engineering".into(), 2.into(), 50.into()]),
            Tuple::from_fields(vec!["Marketing".into(), 0.into(), Field::Int(None)]),
        ];
        check_result(result, &mut expected, true, true);
    }

    #[rstest]
    #[case::volcano(volcano_executor)]
    #[case::inmem_pipeline(inmem_pipeline_executor)]
    #[case::ondisk_pipeline(ondisk_pipeline_executor)]
    fn test_correlated_exists<B: TupleBuffer>(#[case] exec: impl Fn(&str) -> Arc<B>) {
        let sql_string = "SELECT d.name FROM Departments d WHERE EXISTS ( SELECT 1 FROM Employees e WHERE e.department_id = d.id ); ";
        let result = exec(sql_string);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into()]),
            Tuple::from_fields(vec!["Engineering".into()]),
        ];
        check_result(result, &mut expected, true, true);
    }

    #[rstest]
    #[case::volcano(volcano_executor)]
    #[case::inmem_pipeline(inmem_pipeline_executor)]
    #[case::ondisk_pipeline(ondisk_pipeline_executor)]
    fn test_uncorrelated_exists<B: TupleBuffer>(#[case] exec: impl Fn(&str) -> Arc<B>) {
        let sql_string =
            "SELECT d.name FROM Departments d WHERE EXISTS ( SELECT 1 FROM Employees ); ";
        let result = exec(sql_string);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into()]),
            Tuple::from_fields(vec!["Engineering".into()]),
            Tuple::from_fields(vec!["Marketing".into()]),
        ];
        check_result(result, &mut expected, true, true);
    }

    #[rstest]
    #[case::volcano(volcano_executor)]
    #[case::inmem_pipeline(inmem_pipeline_executor)]
    #[case::ondisk_pipeline(ondisk_pipeline_executor)]
    fn test_orderby<B: TupleBuffer>(#[case] exec: impl Fn(&str) -> Arc<B>) {
        let sql_string = "SELECT name, age FROM Employees ORDER BY age DESC, name ASC";
        let result = exec(sql_string);
        let mut expected = vec![
            Tuple::from_fields(vec!["Eva".into(), 40.into()]),
            Tuple::from_fields(vec!["Charlie".into(), 35.into()]),
            Tuple::from_fields(vec!["Alice".into(), 30.into()]),
            Tuple::from_fields(vec!["David".into(), 28.into()]),
            Tuple::from_fields(vec!["Bob".into(), 22.into()]),
        ];
        check_result(result, &mut expected, false, false);
    }

    #[rstest]
    #[case::volcano(volcano_executor)]
    #[case::inmem_pipeline(inmem_pipeline_executor)]
    #[case::ondisk_pipeline(ondisk_pipeline_executor)]
    fn test_orderby_expression<B: TupleBuffer>(#[case] exec: impl Fn(&str) -> Arc<B>) {
        let sql_string = "SELECT name, age FROM Employees ORDER BY age + 1 DESC";
        let result = exec(sql_string);
        let mut expected = vec![
            Tuple::from_fields(vec!["Eva".into(), 40.into()]),
            Tuple::from_fields(vec!["Charlie".into(), 35.into()]),
            Tuple::from_fields(vec!["Alice".into(), 30.into()]),
            Tuple::from_fields(vec!["David".into(), 28.into()]),
            Tuple::from_fields(vec!["Bob".into(), 22.into()]),
        ];
        check_result(result, &mut expected, false, false);
    }

    #[rstest]
    #[case::volcano(volcano_executor)]
    #[case::inmem_pipeline(inmem_pipeline_executor)]
    #[case::ondisk_pipeline(ondisk_pipeline_executor)]
    fn test_groupby_orderby<B: TupleBuffer>(#[case] exec: impl Fn(&str) -> Arc<B>) {
        let sql_string = "SELECT d.name, AVG(e.age) AS average_age FROM Employees e JOIN Departments d ON e.department_id = d.id GROUP BY d.name ORDER BY average_age DESC";
        let result = exec(sql_string);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into(), 32.5.into()]),
            Tuple::from_fields(vec!["Engineering".into(), 25.0.into()]),
        ];
        check_result(result, &mut expected, false, false);
    }

    #[rstest]
    #[case::volcano(volcano_executor)]
    #[case::inmem_pipeline(inmem_pipeline_executor)]
    #[case::ondisk_pipeline(ondisk_pipeline_executor)]
    fn test_in_subquery<B: TupleBuffer>(#[case] exec: impl Fn(&str) -> Arc<B>) {
        let sql_string = "SELECT name FROM Employees WHERE department_id IN (SELECT id FROM Departments WHERE name = 'HR')";
        let result = exec(sql_string);
        let mut expected = vec![
            Tuple::from_fields(vec!["Alice".into()]),
            Tuple::from_fields(vec!["Charlie".into()]),
        ];
        check_result(result, &mut expected, true, false);

        let sql_string = "SELECT name FROM Employees WHERE department_id IN (SELECT id FROM Departments WHERE name = 'Marketing')";
        let result = exec(sql_string);
        let mut expected = vec![];
        check_result(result, &mut expected, true, false);

        let sql_string =
            "SELECT name FROM Departments WHERE id IN (SELECT department_id FROM Employees)";
        let result = exec(sql_string);
        let mut expected = vec![
            Tuple::from_fields(vec!["HR".into()]),
            Tuple::from_fields(vec!["Engineering".into()]),
        ];
        check_result(result, &mut expected, true, false);
    }
}
