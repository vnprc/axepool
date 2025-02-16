use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, AsyncBufReadExt};
use tokio::sync::{broadcast, mpsc, Mutex};
use serde_json::{Value, json};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::error::Error;
use std::env;

// Structure to hold upstream job parameters.
#[derive(Clone, Debug)]
struct JobParams {
    coinb1: String,
    coinb2: String,
    full_extranonce1: String,
    extranonce2_size: usize,
}

// Shared state to store upstream job parameters.
type SharedJobParams = Arc<Mutex<Option<JobParams>>>;

// Shared state to cache the most recent mining.notify message.
type SharedLastNotify = Arc<Mutex<Option<String>>>;

// An atomic counter to assign each miner a unique constrained extranonce.
static MINER_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Expect upstream address and worker name from the command line.
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <upstream_address:port> <worker_name>", args[0]);
        std::process::exit(1);
    }
    let upstream_addr = args[1].clone();
    let worker_name = args[2].clone();
    println!("Using upstream address: {}", upstream_addr);
    println!("Using worker name for upstream subscription: {}", worker_name);

    // Connect to the upstream pool.
    let mut upstream_stream = TcpStream::connect(&upstream_addr).await?;
    println!("Connected to upstream at {}", upstream_addr);

    // Send subscribe request to upstream.
    let subscribe_message = json!({
        "id": 1,
        "method": "mining.subscribe",
        "params": []
    })
    .to_string();
    upstream_stream.write_all(subscribe_message.as_bytes()).await?;
    upstream_stream.write_all(b"\n").await?;
    upstream_stream.flush().await?;
    println!("Sent subscribe request to upstream.");

    // Send authorize request using the worker name passed via CLI.
    let authorize_message = json!({
        "id": 2,
        "method": "mining.authorize",
        "params": [worker_name, "x"]  // Replace "x" with a password if needed.
    })
    .to_string();
    upstream_stream.write_all(authorize_message.as_bytes()).await?;
    upstream_stream.write_all(b"\n").await?;
    upstream_stream.flush().await?;
    println!("Sent authorize request to upstream.");

    // Split the upstream connection.
    let (upstream_reader, upstream_writer) = upstream_stream.into_split();

    // Shared state for upstream job parameters.
    let job_params: SharedJobParams = Arc::new(Mutex::new(None));
    // Shared state for the latest mining.notify message.
    let last_notify: SharedLastNotify = Arc::new(Mutex::new(None));

    // Broadcast channel for job (mining.notify) messages.
    let (job_tx, _) = broadcast::channel::<String>(16);
    // MPSC channel for share submissions forwarded upstream.
    let (share_tx, share_rx) = mpsc::channel::<String>(16);

    // Spawn task to read from upstream.
    {
        let job_tx = job_tx.clone();
        let job_params = job_params.clone();
        let last_notify = last_notify.clone();
        tokio::spawn(async move {
            upstream_read_handler(upstream_reader, job_tx, job_params, last_notify).await;
        });
    }

    // Spawn task to write share submissions to upstream.
    {
        tokio::spawn(async move {
            upstream_write_handler(upstream_writer, share_rx).await;
        });
    }

    // Listen for downstream miner connections.
    let local_addr = "0.0.0.0:3334"; // A port separate from upstream.
    let listener = TcpListener::bind(local_addr).await?;
    println!("Listening for miners on {}", local_addr);

    loop {
        let (miner_socket, addr) = listener.accept().await?;
        println!("Accepted miner connection from {}", addr);
        let job_tx = job_tx.clone();
        let share_tx = share_tx.clone();
        let job_params = job_params.clone();
        let last_notify = last_notify.clone();
        tokio::spawn(async move {
            handle_miner(miner_socket, job_tx.subscribe(), share_tx, job_params, last_notify).await;
        });
    }
}

/// Reads messages from upstream.
/// - For each mining.notify, updates the cached notify and broadcasts it.
/// - For the subscribe response (id==1), extracts extranonce parameters.
/// - For the authorize response (id==2) that succeeds, broadcasts the cached mining.notify.
async fn upstream_read_handler(
    mut reader: tokio::net::tcp::OwnedReadHalf,
    job_tx: broadcast::Sender<String>,
    job_params: SharedJobParams,
    last_notify: SharedLastNotify,
) {
    let mut buf_reader = BufReader::new(reader);
    let mut line = String::new();
    loop {
        line.clear();
        match buf_reader.read_line(&mut line).await {
            Ok(0) => {
                println!("Upstream closed connection");
                break;
            }
            Ok(_) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                println!("Upstream message: {}", trimmed);
                if let Ok(val) = serde_json::from_str::<Value>(trimmed) {
                    // Check if it's a mining.notify.
                    if let Some(method) = val.get("method").and_then(|m| m.as_str()) {
                        if method == "mining.notify" {
                            {
                                // Cache the most recent notify.
                                let mut last_lock = last_notify.lock().await;
                                *last_lock = Some(trimmed.to_string());
                            }
                            let _ = job_tx.send(trimmed.to_string());
                            continue;
                        }
                    }
                    // Check for subscribe (id==1) response.
                    if let Some(id) = val.get("id").and_then(|v| v.as_u64()) {
                        if id == 1 {
                            if let Some(result) = val.get("result").and_then(|r| r.as_array()) {
                                if result.len() >= 3 {
                                    if let (Some(full_extranonce1), Some(extranonce2_size)) = (
                                        result.get(1).and_then(|v| v.as_str()),
                                        result.get(2).and_then(|v| v.as_u64()),
                                    ) {
                                        let params = JobParams {
                                            coinb1: "".to_string(),
                                            coinb2: "".to_string(),
                                            full_extranonce1: full_extranonce1.to_string(),
                                            extranonce2_size: extranonce2_size as usize,
                                        };
                                        let mut lock = job_params.lock().await;
                                        *lock = Some(params);
                                        println!("Stored upstream job parameters: {:?}", *lock);
                                    }
                                }
                            }
                        } else if id == 2 {
                            // This is the authorize response.
                            // If authorize was successful (result==true), broadcast the cached notify.
                            if let Some(result) = val.get("result") {
                                if result.as_bool().unwrap_or(false) {
                                    let cached = {
                                        let lock = last_notify.lock().await;
                                        lock.clone()
                                    };
                                    if let Some(notify_msg) = cached {
                                        println!("Broadcasting cached mining.notify after authorize: {}", notify_msg);
                                        let _ = job_tx.send(notify_msg);
                                    }
                                } else {
                                    println!("Authorize failed: {}", trimmed);
                                }
                            }
                        }
                    }
                } else {
                    println!("Failed to parse JSON: {}", trimmed);
                }
            }
            Err(e) => {
                println!("Error reading from upstream: {:?}", e);
                break;
            }
        }
    }
}

/// Writes share submissions to the upstream pool.
async fn upstream_write_handler(
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    mut rx: mpsc::Receiver<String>,
) {
    while let Some(share) = rx.recv().await {
        println!("Forwarding share to upstream: {}", share);
        if let Err(e) = writer.write_all(share.as_bytes()).await {
            println!("Error writing share to upstream: {:?}", e);
            break;
        }
        if let Err(e) = writer.write_all(b"\n").await {
            println!("Error writing newline to upstream: {:?}", e);
            break;
        }
    }
}

/// Handles a miner connection:
/// - Responds to the miner’s subscribe with a constrained extranonce.
/// - Sends the current cached mining.notify if available.
/// - Forwards mining.notify messages (modified for the miner) and transforms share submissions.
async fn handle_miner(
    socket: TcpStream,
    mut job_rx: broadcast::Receiver<String>,
    share_tx: mpsc::Sender<String>,
    job_params: SharedJobParams,
    last_notify: SharedLastNotify,
) {
    let miner_id = MINER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let constrained_extranonce = format!("{:04x}", miner_id);
    println!("Assigned constrained extranonce {} to miner {}", constrained_extranonce, miner_id);

    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    // Read the miner’s subscribe request.
    if let Ok(n) = reader.read_line(&mut line).await {
        if n > 0 {
            println!("Miner {} sent: {}", miner_id, line.trim_end());
            // Wait for upstream parameters.
            let upstream_params = loop {
                let lock = job_params.lock().await;
                if let Some(ref params) = *lock {
                    break params.clone();
                }
                drop(lock);
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            };

            // Build and send the subscribe response.
            let subscribe_response = json!({
                "id": null,
                "result": [
                    [
                        ["mining.set_difficulty", "1"],
                        ["mining.notify", "1"]
                    ],
                    upstream_params.full_extranonce1.clone() + &constrained_extranonce,
                    upstream_params.extranonce2_size - 2
                ],
                "error": null
            });
            let response_str = serde_json::to_string(&subscribe_response).unwrap();
            if let Err(e) = writer.write_all(response_str.as_bytes()).await {
                println!("Error writing subscribe response to miner {}: {:?}", miner_id, e);
                return;
            }
            if let Err(e) = writer.write_all(b"\n").await {
                println!("Error writing newline to miner {}: {:?}", miner_id, e);
                return;
            }
            if let Err(e) = writer.flush().await {
                println!("Error flushing writer for miner {}: {:?}", miner_id, e);
                return;
            }
            line.clear();

            // Immediately send the current mining.notify (if cached) after subscribe.
            let cached_notify = {
                let lock = last_notify.lock().await;
                lock.clone()
            };
            if let Some(notify) = cached_notify {
                println!("Sending cached mining.notify to miner {}: {}", miner_id, notify);
                if let Err(e) = writer.write_all(notify.as_bytes()).await {
                    println!("Error sending notify to miner {}: {:?}", miner_id, e);
                }
                if let Err(e) = writer.write_all(b"\n").await {
                    println!("Error writing newline to miner {}: {:?}", miner_id, e);
                }
            }
        }
    } else {
        println!("Failed to read subscribe request from miner {}", miner_id);
        return;
    }

    // Process job notifications and miner messages concurrently.
    loop {
        tokio::select! {
            Ok(job_msg) = job_rx.recv() => {
                let modified_job = modify_job_for_miner(&job_msg, &constrained_extranonce);
                if let Err(e) = writer.write_all(modified_job.as_bytes()).await {
                    println!("Error sending job to miner {}: {:?}", miner_id, e);
                    break;
                }
                if let Err(e) = writer.write_all(b"\n").await {
                    println!("Error writing newline to miner {}: {:?}", miner_id, e);
                    break;
                }
            }
            result = reader.read_line(&mut line) => {
                match result {
                    Ok(0) => {
                        println!("Miner {} disconnected", miner_id);
                        break;
                    }
                    Ok(_) => {
                        println!("Received from miner {}: {}", miner_id, line.trim_end());
                        let upstream_params = {
                            let lock = job_params.lock().await;
                            if let Some(ref params) = *lock {
                                params.clone()
                            } else {
                                println!("No upstream parameters available for miner {}", miner_id);
                                continue;
                            }
                        };
                        let transformed_share = transform_share_submission(
                            &line,
                            &constrained_extranonce,
                            &upstream_params.full_extranonce1
                        );
                        if let Err(e) = share_tx.send(transformed_share).await {
                            println!("Error sending share from miner {} upstream: {:?}", miner_id, e);
                            break;
                        }
                        line.clear();
                    }
                    Err(e) => {
                        println!("Error reading from miner {}: {:?}", miner_id, e);
                        break;
                    }
                }
            }
        }
    }
}

/// Modifies a mining.notify message by injecting the miner’s constrained extranonce.
fn modify_job_for_miner(job: &str, constrained_extranonce: &str) -> String {
    if let Ok(mut value) = serde_json::from_str::<Value>(job) {
        if let Some(method) = value.get("method").and_then(|m| m.as_str()) {
            if method == "mining.notify" {
                if let Some(params) = value.get_mut("params").and_then(|p| p.as_array_mut()) {
                    if params.len() >= 3 {
                        if let (Some(coinb1), Some(coinb2)) =
                            (params.get(1).and_then(|v| v.as_str()), params.get(2).and_then(|v| v.as_str()))
                        {
                            let modified_coinbase = format!("{}{}{}", coinb1, constrained_extranonce, coinb2);
                            params[1] = Value::String(modified_coinbase);
                        }
                    }
                }
            }
        }
        serde_json::to_string(&value).unwrap_or_else(|_| job.to_string())
    } else {
        job.to_string()
    }
}

/// Transforms a miner’s share submission by replacing the constrained extranonce with the full upstream extranonce.
fn transform_share_submission(submission: &str, constrained_extranonce: &str, full_extranonce: &str) -> String {
    if let Ok(mut value) = serde_json::from_str::<Value>(submission) {
        if let Some(method) = value.get("method").and_then(|m| m.as_str()) {
            if method == "mining.submit" {
                if let Some(params) = value.get_mut("params").and_then(|p| p.as_array_mut()) {
                    if params.len() >= 2 {
                        if let Some(job_id) = params.get(1).and_then(|v| v.as_str()) {
                            let transformed_job_id = job_id.replace(constrained_extranonce, full_extranonce);
                            params[1] = Value::String(transformed_job_id);
                        }
                    }
                }
            }
        }
        serde_json::to_string(&value).unwrap_or_else(|_| submission.to_string())
    } else {
        submission.to_string()
    }
}
