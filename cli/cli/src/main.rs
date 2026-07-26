// Copyright 2026 MonoTS Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use arrow::array::{Array, AsArray};
use arrow::record_batch::RecordBatch;
use clap::Parser;
use comfy_table::{presets::UTF8_FULL, Table};
use common::{print_banner, LogConfig, LogGuard};
use monots_core::config::AppConfig;
use monots_core::sql::{route_sql, SqlRoute};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use sdk::Client;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "monots", about = "Edge TSDB CLI (SQL)")]
struct Args {
    #[arg(short = 'H', long, default_value = "http://127.0.0.1:50051")]
    host: String,

    #[arg(short, long, default_value = "admin")]
    user: String,

    #[arg(short, long, default_value = "admin")]
    password: String,

    /// YAML config (`logging` section controls CLI log output).
    #[arg(long)]
    config: Option<PathBuf>,

    #[arg(long)]
    sql: Option<String>,
}

fn config_base(config_path: &std::path::Path) -> PathBuf {
    if let Ok(home) = std::env::var("MONOTS_HOME") {
        return PathBuf::from(home);
    }
    config_path
        .parent()
        .and_then(|p| p.parent())
        .unwrap_or_else(|| std::path::Path::new("."))
        .to_path_buf()
}

fn init_cli_logging(args: &Args) {
    if let Some(path) = AppConfig::resolve_path(args.config.clone()) {
        if let Ok(app) = AppConfig::load(&path) {
            let base = config_base(&path);
            let log_dir = app.resolve_log_dir(&base);
            let mut logging = app.logging.clone();
            logging.file = false;
            LogGuard::init(&logging, &log_dir);
            return;
        }
    }
    LogGuard::init(&LogConfig::cli_default(), std::path::Path::new("."));
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    init_cli_logging(&args);
    run_sql_shell(&args).await?;
    Ok(())
}

async fn run_sql_shell(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = connect(args).await?;

    if let Some(sql) = &args.sql {
        run_query(&mut client, sql).await?;
        return Ok(());
    }

    let mut rl = DefaultEditor::new()?;
    print_banner();
    println!("Welcome to MonoTS CLI.");
    println!("Stream DDL: CREATE/DROP STREAM (NoQuery), SHOW STREAM* (Query).");

    loop {
        let readline = rl.readline("monots> ");
        match readline {
            Ok(line) => {
                let sql = line.trim();
                if sql.eq_ignore_ascii_case("exit") {
                    break;
                }
                if sql.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(sql);
                if let Err(e) = run_query(&mut client, sql).await {
                    println!("Error: {e}");
                }
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => break,
            Err(err) => {
                println!("Error: {err:?}");
                break;
            }
        }
    }
    Ok(())
}

async fn connect(args: &Args) -> Result<Client, Box<dyn std::error::Error>> {
    let mut client = Client::connect(&args.host).await?;
    client.login(&args.user, &args.password).await?;
    Ok(client)
}

async fn run_query(client: &mut Client, sql: &str) -> Result<(), Box<dyn std::error::Error>> {
    match route_sql(sql).map_err(|e| e.to_string())? {
        SqlRoute::NoQuery(_) => {
            let rows = client.no_query(sql).await?;
            if rows > 0 {
                println!("OK ({rows} rows affected)");
            } else {
                println!("OK");
            }
        }
        SqlRoute::Query => {
            let batches = client.query(sql).await?;
            if batches.is_empty() {
                println!("OK");
                return Ok(());
            }
            print_batches(&batches);
        }
    }
    Ok(())
}

fn print_batches(batches: &[RecordBatch]) {
    let schema = batches[0].schema();
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_header(schema.fields().iter().map(|f| f.name().as_str()));

    for batch in batches {
        for row in 0..batch.num_rows() {
            let values: Vec<String> = (0..batch.num_columns())
                .map(|col| array_value_at(batch.column(col).as_ref(), row))
                .collect();
            table.add_row(values);
        }
    }
    println!("{table}");
}

fn array_value_at(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "NULL".to_string();
    }
    match array.data_type() {
        arrow::datatypes::DataType::Int64 => format!(
            "{}",
            array
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(row)
        ),
        arrow::datatypes::DataType::Float64 => format!(
            "{}",
            array
                .as_primitive::<arrow::datatypes::Float64Type>()
                .value(row)
        ),
        arrow::datatypes::DataType::Boolean => format!("{}", array.as_boolean().value(row)),
        arrow::datatypes::DataType::Utf8 => array.as_string::<i32>().value(row).to_string(),
        arrow::datatypes::DataType::Timestamp(_, _) => format!(
            "{}",
            array
                .as_primitive::<arrow::datatypes::TimestampMillisecondType>()
                .value(row)
        ),
        other => format!("<{other:?}>"),
    }
}
