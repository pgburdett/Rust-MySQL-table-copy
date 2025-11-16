
use mysql::*;
use mysql::prelude::*;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use chrono::NaiveDateTime;
use std::error::Error;
use futures::stream::{StreamExt, FuturesUnordered};
use dialoguer::{Input, Select, MultiSelect, theme::ColorfulTheme};
use std::time::{Instant, Duration};
 
const BATCH_SIZE: usize = 10000;
const BUFFER_SIZE: usize = 1000;
const MAX_CONCURRENT_TABLES: usize = 4;
const RETRY_ATTEMPTS: u32 = 3;
const RETRY_DELAY: Duration = Duration::from_secs(2);
const DROP_EXISTING: bool = true;
 
#[derive(Debug)]
struct TableReference {
    table_name: String,
    referenced_tables: Vec<String>,
}
 
#[derive(Debug)]
struct TableStats {
    row_count: i64,
    size_mb: f64,
    estimated_time: Duration,
}
 
struct InsertBuffer {
    rows: VecDeque<Vec<Value>>,
    capacity: usize,
}
 
impl InsertBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            rows: VecDeque::with_capacity(capacity),
            capacity,
        }
    }
 
    fn push(&mut self, row: Vec<Value>) -> bool {
        self.rows.push_back(row);
        self.rows.len() >= self.capacity
    }
 
    fn flush(&mut self) -> Vec<Vec<Value>> {
        let mut rows = VecDeque::with_capacity(self.capacity);
        std::mem::swap(&mut self.rows, &mut rows);
        rows.into_iter().collect()
    }
}
 
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("MySQL Database Copier v2.0");
    println!("-------------------------");
 
    let source_url = Input::<String>::new()
        .with_prompt("Source database URL")
        .default("mysql://root:password@localhost:3306/source_db".to_string())
        .interact()?;
    
    let target_url = Input::<String>::new()
        .with_prompt("Target database URL")
        .default("mysql://root:password@localhost:3306/target_db".to_string())
        .interact()?;
    
    let source_pool = Pool::new(&source_url)?;
    let target_pool = Pool::new(&target_url)?;
    
    let all_tables = get_sorted_tables(&source_pool)?;
    
    println!("\nAvailable tables (in dependency order):");
    println!("-------------------------------------");
    
    let mut total_size = 0.0;
    for (i, table) in all_tables.iter().enumerate() {
        let stats = get_table_stats(&source_pool, table)?;
        total_size += stats.size_mb;
        println!("{}. {} ({:.2} MB, {} rows, est. time: {:.1}s)", 
            i + 1, table, stats.size_mb, stats.row_count, stats.estimated_time.as_secs_f64());
    }
    
    println!("\nTotal size: {:.2} MB", total_size);
 
    let options = vec!["Copy all tables", "Select specific tables"];
    let selection = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("\nSelect operation mode")
        .items(&options)
        .default(0)
        .interact()?;
    
    let tables_to_copy = if selection == 0 {
        all_tables.clone()
    } else {
        let selections = MultiSelect::with_theme(&ColorfulTheme::default())
            .with_prompt("Select tables to copy")
            .items(&all_tables)
            .interact()?;
            
        selections.into_iter()
            .map(|i| all_tables[i].clone())
            .collect()
    };
    
    let start_time = Instant::now();
    let multi_progress = Arc::new(MultiProgress::new());
    
    let total_tables = tables_to_copy.len();
    let overall_progress = multi_progress.add(ProgressBar::new(total_tables as u64));
    overall_progress.set_style(ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} tables ({percent}%) {msg}")
        .progress_chars("##-"));
    overall_progress.set_message("Overall progress");
    
    let buffer = Arc::new(Mutex::new(InsertBuffer::new(BUFFER_SIZE)));
    
    let result = copy_tables_parallel(
        &source_pool, 
        &target_pool, 
        &tables_to_copy, 
        multi_progress.clone(),
        overall_progress.clone(),
        buffer
    ).await;
    
    match result {
        Ok(_) => {
            let duration = start_time.elapsed();
            println!("\nCopy completed successfully!");
            println!("Total time: {:.2} seconds", duration.as_secs_f64());
            println!("Average speed: {:.2} MB/s", total_size / duration.as_secs_f64());
            println!("Tables copied: {}", total_tables);
        },
        Err(e) => {
            println!("\nError during copy: {}", e);
            println!("Check logs for details");
        }
    }
    
    Ok(())
}
 
// ... [Previous functions: copy_tables_parallel, copy_table, handle_datetime_value, etc.]
async fn copy_tables_parallel(
    source_pool: &Pool,
    target_pool: &Pool,
    tables: &[String],
    multi_progress: Arc<MultiProgress>,
    overall_progress: ProgressBar,
    buffer: Arc<Mutex<InsertBuffer>>,
) -> Result<(), Box<dyn Error>> {
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_TABLES));
    let mut tasks = FuturesUnordered::new();
 
    for table in tables {
        let table = table.clone();
        let source_pool = source_pool.clone();
        let target_pool = target_pool.clone();
        let semaphore = Arc::clone(&semaphore);
        let multi_progress = Arc::clone(&multi_progress);
        let buffer = Arc::clone(&buffer);
 
        let task = tokio::spawn(async move {
            let _permit = semaphore.acquire().await?;
            let result = copy_table_with_retry(
                &source_pool, 
                &target_pool, 
                &table, 
                &multi_progress,
                &buffer
            ).await;
            result
        });
 
        tasks.push(task);
    }
 
    while let Some(result) = tasks.next().await {
        result??;
        overall_progress.inc(1);
    }
 
    overall_progress.finish_with_message("All tables copied successfully!");
    Ok(())
}
 
async fn copy_table_with_retry(
    source_pool: &Pool,
    target_pool: &Pool,
    table: &str,
    multi_progress: &MultiProgress,
    buffer: &Arc<Mutex<InsertBuffer>>,
) -> Result<(), Box<dyn Error>> {
    let mut attempts = 0;
    loop {
        match copy_table(source_pool, target_pool, table, multi_progress, buffer).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                attempts += 1;
                if attempts >= RETRY_ATTEMPTS {
                    return Err(e);
                }
                println!("Retrying table {} ({}/{}): {}", 
                    table, attempts, RETRY_ATTEMPTS, e);
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }
}
 
async fn copy_table(
    source_pool: &Pool,
    target_pool: &Pool,
    table: &str,
    multi_progress: &MultiProgress,
    buffer: &Arc<Mutex<InsertBuffer>>,
) -> Result<(), Box<dyn Error>> {
    let mut source_conn = source_pool.get_conn()?;
    let mut target_conn = target_pool.get_conn()?;
 
    // Handle existing table
    let table_exists: bool = target_conn
        .query_first(format!(
            "SELECT 1 FROM information_schema.tables 
             WHERE table_schema = DATABASE() 
             AND table_name = '{}'", 
            table
        ))?
        .unwrap_or(0);
 
    if table_exists {
        if DROP_EXISTING {
            target_conn.query_drop(format!("DROP TABLE {}", table))?;
        } else {
            target_conn.query_drop(format!("TRUNCATE TABLE {}", table))?;
        }
    }
 
    // Create table
    let create_stmt: String = source_conn
        .query_first(format!("SHOW CREATE TABLE {}", table))?
        .unwrap_or_default();
    target_conn.query_drop(&create_stmt)?;
 
    let count: i64 = source_conn
        .query_first(format!("SELECT COUNT(*) FROM {}", table))?
        .unwrap_or(0);
 
    let progress = multi_progress.add(ProgressBar::new(count as u64));
    progress.set_style(ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({percent}%) {msg}")
        .progress_chars("##-"));
    progress.set_message(format!("Copying {}", table));
 
    let mut offset = 0;
    loop {
        let batch: Vec<Row> = source_conn.query(format!(
            "SELECT * FROM {} LIMIT {}, {}",
            table, offset, BATCH_SIZE
        ))?;
 
        if batch.is_empty() {
            break;
        }
 
        for row in batch {
            let mut row_values = Vec::new();
            for (i, column) in row.columns().iter().enumerate() {
                let value = match row.get_raw(i) {
                    Some(val) => {
                        if column.column_type() == ColumnType::DATETIME {
                            handle_datetime_value(val)?
                        } else {
                            val.clone()
                        }
                    }
                    None => Value::NULL,
                };
                row_values.push(value);
            }
 
            let mut buffer = buffer.lock().unwrap();
            if buffer.push(row_values) {
                flush_buffer(&mut target_conn, table, buffer.flush())?;
            }
        }
 
        offset += BATCH_SIZE;
        progress.set_position(offset as u64);
    }
 
    // Flush remaining rows
    let mut buffer = buffer.lock().unwrap();
    if !buffer.rows.is_empty() {
        flush_buffer(&mut target_conn, table, buffer.flush())?;
    }
 
    progress.finish_with_message(format!("Completed {}", table));
    Ok(())
}
 
fn handle_datetime_value(val: Value) -> Result<Value, Box<dyn Error>> {
    match val {
        Value::Date(y, m, d, h, i, s, _) => {
            if y == 0 || m == 0 || d == 0 {
                Ok(Value::NULL)
            } else {
                match NaiveDateTime::parse_from_str(
                    &format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, h, i, s),
                    "%Y-%m-%d %H:%M:%S"
                ) {
                    Ok(_) => Ok(val),
                    Err(_) => Ok(Value::NULL),
                }
            }
        }
        _ => Ok(val),
    }
}
 
fn get_sorted_tables(pool: &Pool) -> Result<Vec<String>, Box<dyn Error>> {
    let mut conn = pool.get_conn()?;
    let mut table_refs: Vec<TableReference> = Vec::new();
    
    let tables: Vec<String> = conn.query("SHOW TABLES")?;
    
    for table in &tables {
        let refs: Vec<String> = conn.query(format!(
            "SELECT REFERENCED_TABLE_NAME 
             FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
             WHERE TABLE_SCHEMA = DATABASE() 
             AND TABLE_NAME = '{}' 
             AND REFERENCED_TABLE_NAME IS NOT NULL", table))?;
        
        table_refs.push(TableReference {
            table_name: table.clone(),
            referenced_tables: refs,
        });
    }
    
    Ok(topological_sort(table_refs)?)
}
 
fn topological_sort(table_refs: Vec<TableReference>) -> Result<Vec<String>, Box<dyn Error>> {
    let mut result = Vec::new();
    let mut visited = HashSet::new();
    let mut temp_visited = HashSet::new();
    
    let ref_map: HashMap<_, _> = table_refs.iter()
        .map(|r| (r.table_name.clone(), r.referenced_tables.clone()))
        .collect();
    
    fn visit(
        table: &str,
        ref_map: &HashMap<String, Vec<String>>,
        visited: &mut HashSet<String>,
        temp_visited: &mut HashSet<String>,
        result: &mut Vec<String>,
    ) -> Result<(), Box<dyn Error>> {
        if temp_visited.contains(table) {
            return Err("Circular dependency detected".into());
        }
        if visited.contains(table) {
            return Ok(());
        }
        
        temp_visited.insert(table.to_string());
        
        if let Some(refs) = ref_map.get(table) {
            for ref_table in refs {
                visit(ref_table, ref_map, visited, temp_visited, result)?;
            }
        }
        
        temp_visited.remove(table);
        visited.insert(table.to_string());
        result.push(table.to_string());
        
        Ok(())
    }
    
    for table in ref_map.keys() {
        if !visited.contains(table.as_str()) {
            visit(table, &ref_map, &mut visited, &mut temp_visited, &mut result)?;
        }
    }
    
    Ok(result)
}
 
fn get_table_stats(pool: &Pool, table: &str) -> Result<TableStats, Box<dyn Error>> {
    let mut conn = pool.get_conn()?;
    let (row_count, size_bytes): (i64, i64) = conn.query_first(format!(
        "SELECT table_rows, data_length + index_length 
         FROM information_schema.tables 
         WHERE table_schema = DATABASE() 
         AND table_name = '{}'", table
    ))?.unwrap_or((0, 0));
 
    let size_mb = size_bytes as f64 / (1024.0 * 1024.0);
    let estimated_time = Duration::from_secs((size_mb * 0.5) as u64); // Rough estimate
 
    Ok(TableStats {
        row_count,
        size_mb,
        estimated_time,
    })
}
 
fn flush_buffer(
    conn: &mut Conn,
    table: &str,
    rows: Vec<Vec<Value>>,
) -> Result<(), Box<dyn Error>> {
    if rows.is_empty() {
        return Ok(());
    }
 
    let placeholders = format!(
        "({})",
        (0..rows[0].len())
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ")
    );
 
    let values_str = format!(
        "{}",
        vec![placeholders.as_str(); rows.len()].join(", ")
    );
 
    let insert_query = format!(
        "INSERT INTO {} VALUES {}",
        table, values_str
    );
 
    let flat_values: Vec<Value> = rows.into_iter().flatten().collect();
    conn.exec_batch(&insert_query, flat_values)?;
 
    Ok(())
}
```
