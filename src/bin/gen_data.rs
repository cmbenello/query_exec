use rand::distributions::{Distribution, Uniform};
use rand::Rng;
use rand_distr::Normal as RandNormal;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, BufRead, Write};
use std::path::Path;
// use rand::seq::SliceRandom;
use chrono::{Duration, NaiveDate};
use clap::Parser;
use rand::thread_rng;
use regex::Regex;

// Define CLI arguments using the derive feature
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Sets the schema file to use
    #[clap(short, long, value_parser)]
    schema: String,

    /// Sets the output CSV file
    #[clap(short, long, value_parser)]
    output: String,

    /// Sets the output SQL schema file
    #[clap(short = 'q', long, value_parser)]
    sql_output: Option<String>,
}

// Data type definitions
#[derive(Debug, Clone)]
enum DataType {
    Integer,
    Float,
    VarChar(usize),
    Char(usize),
    Date,
    Boolean,
}

// Distribution types
#[derive(Debug, Clone)]
enum DistributionType {
    Uniform(i64, i64),                // min, max
    Normal(f64, f64),                 // mean, stddev
    Bimodal(f64, f64, f64, f64, f64), // mean1, stddev1, mean2, stddev2, probability of first mode
    Zipf(f64),                        // skew factor
    Sequential(i64),                  // starting point
    Reference(String),                // reference to another column
    Categorical(Vec<(String, f64)>),  // values and their probabilities
}

// Column constraints
#[derive(Debug, Clone)]
enum Constraint {
    PrimaryKey,
    NotNull,
    Unique,
    None,
}

// Column definition
#[derive(Debug, Clone)]
struct Column {
    name: String,
    data_type: DataType,
    distribution: DistributionType,
    constraint: Constraint,
}

// Table definition
#[derive(Debug)]
struct Table {
    name: String,
    columns: Vec<Column>,
    num_tuples: usize,
}

// Parse the modified schema file
fn parse_schema_file(file_path: &str) -> Result<Table, Box<dyn Error>> {
    let file = File::open(file_path)?;
    let reader = io::BufReader::new(file);
    let mut lines = reader.lines();

    // Parse the first line to get number of tuples and table name
    let first_line = lines.next().ok_or("Empty file")??;
    let parts: Vec<&str> = first_line.trim().split_whitespace().collect();
    if parts.len() < 3 || parts[1] != "CREATE" || parts[2] != "TABLE" {
        return Err("Invalid schema format: must start with 'N CREATE TABLE'".into());
    }

    let num_tuples = parts[0].parse::<usize>()?;
    let table_name = parts[3].trim_end_matches('(').to_string();

    // Parse columns
    let mut columns = Vec::new();
    let mut in_columns_section = false;

    for line_result in lines {
        let line = line_result?;
        let trimmed = line.trim();

        if !in_columns_section && trimmed.starts_with('(') {
            in_columns_section = true;
            continue;
        }

        if trimmed.is_empty() || trimmed == ")" {
            continue;
        }

        // Remove trailing comma if present
        let column_def = trimmed.trim_end_matches(',');

        // Parse column definition
        let column = parse_column_definition(column_def)?;
        columns.push(column);
    }

    Ok(Table {
        name: table_name,
        columns,
        num_tuples,
    })
}

// Write data to CSV file
// Updated write_csv function to create directories if they don't exist
fn write_csv(table: &Table, data: &[Vec<String>], output_path: &str) -> Result<(), Box<dyn Error>> {
    // Create parent directories if they don't exist
    if let Some(parent) = Path::new(output_path).parent() {
        fs::create_dir_all(parent)?;
    }

    // Create and open the file
    let mut file = File::create(output_path)?;

    // Write header
    let header: Vec<String> = table.columns.iter().map(|col| col.name.clone()).collect();
    writeln!(file, "{}", header.join(","))?;

    // Write data rows
    for row in data {
        writeln!(file, "{}", row.join(","))?;
    }

    Ok(())
}

// Parse a single column definition
fn parse_column_definition(def: &str) -> Result<Column, Box<dyn Error>> {
    let parts: Vec<&str> = def.split_whitespace().collect();
    if parts.len() < 2 {
        return Err(format!("Invalid column definition: {}", def).into());
    }

    let name = parts[0].to_string();

    // Parse data type
    let data_type_str = parts[1].to_uppercase();
    let data_type = if data_type_str.starts_with("VARCHAR") || data_type_str.starts_with("CHAR") {
        let re = Regex::new(r"\((\d+)\)")?;
        let cap = re
            .captures(&data_type_str)
            .ok_or(format!("Invalid type format: {}", data_type_str))?;
        let size = cap[1].parse::<usize>()?;

        if data_type_str.starts_with("VARCHAR") {
            DataType::VarChar(size)
        } else {
            DataType::Char(size)
        }
    } else if data_type_str == "INTEGER" {
        DataType::Integer
    } else if data_type_str == "FLOAT" {
        DataType::Float
    } else if data_type_str == "DATE" {
        DataType::Date
    } else if data_type_str == "BOOLEAN" {
        DataType::Boolean
    } else {
        return Err(format!("Unsupported data type: {}", data_type_str).into());
    };

    // Parse constraints
    let mut constraint = Constraint::None;
    let mut distribution_idx = 2;

    for i in 2..parts.len() {
        match parts[i].to_uppercase().as_str() {
            "PRIMARY" => {
                if i + 1 < parts.len() && parts[i + 1].to_uppercase() == "KEY" {
                    constraint = Constraint::PrimaryKey;
                    distribution_idx = i + 2;
                    break;
                }
            }
            "NOT" => {
                if i + 1 < parts.len() && parts[i + 1].to_uppercase() == "NULL" {
                    constraint = Constraint::NotNull;
                    distribution_idx = i + 2;
                    break;
                }
            }
            "UNIQUE" => {
                constraint = Constraint::Unique;
                distribution_idx = i + 1;
                break;
            }
            _ => {
                distribution_idx = i;
                break;
            }
        }
    }

    // Parse distribution
    let distribution = if distribution_idx < parts.len() {
        match parts[distribution_idx].to_uppercase().as_str() {
            "UNIFORM" => {
                if distribution_idx + 2 < parts.len() {
                    let min = parts[distribution_idx + 1].parse::<i64>()?;
                    let max = parts[distribution_idx + 2].parse::<i64>()?;
                    DistributionType::Uniform(min, max)
                } else {
                    // Default range for uniform if not specified
                    DistributionType::Uniform(0, 10000)
                }
            }
            "NORMAL" => {
                if distribution_idx + 2 < parts.len() {
                    let mean = parts[distribution_idx + 1].parse::<f64>()?;
                    let stddev = parts[distribution_idx + 2].parse::<f64>()?;
                    DistributionType::Normal(mean, stddev)
                } else {
                    // Default parameters for normal if not specified
                    DistributionType::Normal(0.0, 1.0)
                }
            }
            "BIMODAL" => {
                if distribution_idx + 5 < parts.len() {
                    let mean1 = parts[distribution_idx + 1].parse::<f64>()?;
                    let stddev1 = parts[distribution_idx + 2].parse::<f64>()?;
                    let mean2 = parts[distribution_idx + 3].parse::<f64>()?;
                    let stddev2 = parts[distribution_idx + 4].parse::<f64>()?;
                    let prob = parts[distribution_idx + 5].parse::<f64>()?;
                    DistributionType::Bimodal(mean1, stddev1, mean2, stddev2, prob)
                } else {
                    // Default parameters for bimodal if not specified
                    DistributionType::Bimodal(-5.0, 1.0, 5.0, 1.0, 0.5)
                }
            }
            "ZIPF" => {
                if distribution_idx + 1 < parts.len() {
                    let skew = parts[distribution_idx + 1].parse::<f64>()?;
                    DistributionType::Zipf(skew)
                } else {
                    // Default skew parameter if not specified
                    DistributionType::Zipf(1.0)
                }
            }
            "SEQUENTIAL" => {
                if distribution_idx + 1 < parts.len() {
                    let start = parts[distribution_idx + 1].parse::<i64>()?;
                    DistributionType::Sequential(start)
                } else {
                    DistributionType::Sequential(1)
                }
            }
            _ => {
                // Default to uniform distribution if not recognized
                DistributionType::Uniform(0, 10000)
            }
        }
    } else {
        // Default to uniform distribution if not specified
        DistributionType::Uniform(0, 10000)
    };

    Ok(Column {
        name,
        data_type,
        distribution,
        constraint,
    })
}

// Generate standard SQL schema from the modified schema
fn generate_sql_schema(table: &Table) -> String {
    let mut sql = format!("CREATE TABLE {} (\n", table.name);

    for (i, column) in table.columns.iter().enumerate() {
        let type_str = match &column.data_type {
            DataType::Integer => "INTEGER".to_string(),
            DataType::Float => "FLOAT".to_string(),
            DataType::VarChar(size) => format!("VARCHAR({})", size),
            DataType::Char(size) => format!("CHAR({})", size),
            DataType::Date => "DATE".to_string(),
            DataType::Boolean => "BOOLEAN".to_string(),
        };

        let constraint_str = match &column.constraint {
            Constraint::PrimaryKey => " PRIMARY KEY",
            Constraint::NotNull => " NOT NULL",
            Constraint::Unique => " UNIQUE",
            Constraint::None => "",
        };

        sql.push_str(&format!(
            "    {} {}{}",
            column.name, type_str, constraint_str
        ));

        if i < table.columns.len() - 1 {
            sql.push_str(",\n");
        } else {
            sql.push_str("\n");
        }
    }

    sql.push_str(");");
    sql
}

// Generate random data for a table
// Replace the problematic main function with this fixed version
fn main() -> Result<(), Box<dyn Error>> {
    // Parse command line arguments
    let args = Args::parse();

    // Set default directories
    let schema_dir = "distributions/schemas";
    let data_dir = "distributions/data";

    // Get schema file path - prepend schema directory if not already specified
    let schema_file = if args.schema.contains('/') {
        args.schema.clone()
    } else {
        format!("{}/{}", schema_dir, args.schema)
    };

    // Get output file path - prepend data directory if not already specified
    let output_file = if args.output.contains('/') {
        args.output.clone()
    } else {
        format!("{}/{}", data_dir, args.output)
    };

    // Parse schema
    println!("Parsing schema from file: {}", schema_file);
    let table = parse_schema_file(&schema_file)?;

    // Generate data
    println!(
        "Generating {} tuples for table: {}",
        table.num_tuples, table.name
    );
    let data = generate_data(&table)?;

    // Write to CSV
    println!("Writing data to CSV file: {}", output_file);
    write_csv(&table, &data, &output_file)?;

    // Generate and write SQL schema if requested
    if let Some(sql_output) = &args.sql_output {
        // Default SQL output to data directory if not fully specified
        let sql_file = if sql_output.contains('/') {
            sql_output.clone()
        } else {
            format!("{}/{}", data_dir, sql_output)
        };

        println!("Generating SQL schema and writing to: {}", sql_file);
        let sql_schema = generate_sql_schema(&table);

        // Create parent directories if they don't exist
        if let Some(parent) = Path::new(&sql_file).parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&sql_file, sql_schema)?;
    }

    println!("Done!");
    Ok(())
}

fn generate_data(table: &Table) -> Result<Vec<Vec<String>>, Box<dyn Error>> {
    let mut data = Vec::with_capacity(table.num_tuples);
    let mut rng = thread_rng();

    // For sequential and unique values tracking
    let mut seq_counters: HashMap<String, i64> = HashMap::new();
    let mut unique_values: HashMap<String, Vec<String>> = HashMap::new();

    for _ in 0..table.num_tuples {
        let mut row = Vec::with_capacity(table.columns.len());

        for column in &table.columns {
            let value = match &column.distribution {
                DistributionType::Uniform(min, max) => {
                    match column.data_type {
                        DataType::Integer => {
                            let dist = Uniform::new_inclusive(*min, *max);
                            dist.sample(&mut rng).to_string()
                        }
                        DataType::Float => {
                            let min_f = *min as f64;
                            let max_f = *max as f64;
                            let dist = Uniform::new(min_f, max_f);
                            format!("{:.2}", dist.sample(&mut rng))
                        }
                        // Replace the string generation code in the Uniform match arm
                        DataType::VarChar(size) | DataType::Char(size) => {
                            let length = if let DataType::VarChar(_) = column.data_type {
                                rng.gen_range(1..=size)
                            } else {
                                size
                            };
                            let chars: Vec<char> =
                                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
                                    .chars()
                                    .collect();
                            let random_string: String = (0..length)
                                .map(|_| chars[rng.gen_range(0..chars.len())])
                                .collect();
                            random_string
                        }
                        DataType::Date => {
                            // Generate dates within a reasonable range
                            // Start with a base date and add a random number of days
                            let start_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                            let days_to_add = rng.gen_range(0..=19000); // ~50 years range
                            let random_date = start_date + Duration::days(days_to_add);
                            random_date.format("%Y-%m-%d").to_string()
                        }
                        DataType::Boolean => {
                            if rng.gen_bool(0.5) { "TRUE" } else { "FALSE" }.to_string()
                        }
                    }
                }
                DistributionType::Normal(mean, stddev) => match column.data_type {
                    DataType::Integer => {
                        let dist = RandNormal::new(*mean, *stddev).unwrap();
                        let value = dist.sample(&mut rng).round() as i64;
                        value.to_string()
                    }
                    DataType::Float => {
                        let dist = RandNormal::new(*mean, *stddev).unwrap();
                        format!("{:.2}", dist.sample(&mut rng))
                    }
                    _ => {
                        return Err(format!(
                            "Normal distribution not supported for {}",
                            column.name
                        )
                        .into());
                    }
                },
                DistributionType::Bimodal(mean1, stddev1, mean2, stddev2, prob) => {
                    match column.data_type {
                        DataType::Integer | DataType::Float => {
                            let use_first_dist = rng.gen_bool(*prob);
                            let value = if use_first_dist {
                                let dist = RandNormal::new(*mean1, *stddev1).unwrap();
                                dist.sample(&mut rng)
                            } else {
                                let dist = RandNormal::new(*mean2, *stddev2).unwrap();
                                dist.sample(&mut rng)
                            };

                            // Fix: Separate the conversion and string formatting
                            if let DataType::Integer = column.data_type {
                                (value.round() as i64).to_string()
                            } else {
                                format!("{:.2}", value)
                            }
                        }
                        _ => {
                            return Err(format!(
                                "Bimodal distribution not supported for {}",
                                column.name
                            )
                            .into());
                        }
                    }
                }
                DistributionType::Sequential(start) => {
                    let counter = seq_counters.entry(column.name.clone()).or_insert(*start);
                    let value = counter.to_string();
                    *counter += 1;
                    value
                }
                DistributionType::Zipf(skew) => {
                    // Simple approximation of Zipf distribution for integers
                    match column.data_type {
                        DataType::Integer => {
                            let n = 1000; // Number of possible values
                            let x: f64 = rng.gen_range(1.0..=1.0);
                            let rank = (n as f64 * x.powf(1.0 / skew)) as i64;
                            rank.to_string()
                        }
                        _ => {
                            return Err(format!(
                                "Zipf distribution not supported for {}",
                                column.name
                            )
                            .into());
                        }
                    }
                }

                DistributionType::Categorical(categories) => {
                    // Choose a category based on probabilities
                    let mut cumulative = 0.0;
                    let r = rng.gen_range(0.0..1.0);

                    let mut selected = categories[0].0.clone();
                    for (cat, prob) in categories {
                        cumulative += prob;
                        if r <= cumulative {
                            selected = cat.clone();
                            break;
                        }
                    }
                    selected
                }
                DistributionType::Reference(_) => {
                    // Not implemented in this example
                    "REF_NOT_IMPLEMENTED".to_string()
                }
            };

            // Apply constraints to the generated value
            let final_value = match column.constraint {
                Constraint::Unique | Constraint::PrimaryKey => {
                    let values = unique_values.entry(column.name.clone()).or_default();
                    if values.contains(&value) {
                        // For uniqueness constraints, regenerate value if duplicate
                        let mut new_value = value.clone();
                        let max_attempts = 10;
                        let mut attempts = 0;

                        while values.contains(&new_value) && attempts < max_attempts {
                            // Try to generate a new unique value
                            new_value = match &column.distribution {
                                DistributionType::Uniform(min, max) => {
                                    let dist = Uniform::new_inclusive(*min, *max);
                                    dist.sample(&mut rng).to_string()
                                }
                                DistributionType::Sequential(start) => {
                                    let counter =
                                        seq_counters.entry(column.name.clone()).or_insert(*start);
                                    let val = counter.to_string();
                                    *counter += 1;
                                    val
                                }
                                DistributionType::Normal(mean, stddev) => {
                                    let dist = RandNormal::new(*mean, *stddev).unwrap();
                                    let val = dist.sample(&mut rng);
                                    if let DataType::Integer = column.data_type {
                                        (val.round() as i64).to_string()
                                    } else {
                                        format!("{:.2}", val)
                                    }
                                }
                                _ => format!("{}_unique_{}", value, attempts),
                            };
                            attempts += 1;
                        }

                        // If we still couldn't find a unique value, append a unique suffix
                        if values.contains(&new_value) {
                            new_value = format!("{}_unique_{}", value, rng.gen::<u64>());
                        }

                        values.push(new_value.clone());
                        new_value
                    } else {
                        values.push(value.clone());
                        value
                    }
                }
                Constraint::NotNull => {
                    // Already handled by generating non-null values
                    value
                }
                Constraint::None => {
                    // For non-constrained columns, occasionally insert NULL values
                    if rng.gen_bool(0.05) {
                        // 5% chance of NULL
                        "NULL".to_string()
                    } else {
                        value
                    }
                }
            };

            row.push(final_value);
        }

        data.push(row);
    }

    Ok(data)
}
