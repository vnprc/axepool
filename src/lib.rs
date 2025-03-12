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
    difficulty: f64,
}

// Shared state to store upstream job parameters.
type SharedJobParams = Arc<Mutex<Option<JobParams>>>;

// Shared state to cache the most recent mining.notify message.
type SharedLastNotify = Arc<Mutex<Option<String>>>;

type SharedLastDifficulty = Arc<Mutex<Option<String>>>;

type SharedLastPrevHash = Arc<Mutex<Option<String>>>;

// An atomic counter to assign each miner a unique constrained extranonce.
static MINER_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub async fn run_proxy(
    upstream_addr: &str,
    worker_name: &str,
    on_new_block: Arc<dyn Fn(String) + Send + Sync + 'static>,
    on_share_submitted: Arc<dyn Fn(String) + Send + Sync + 'static>,
) -> Result<(), Box<dyn Error>> {
    // Connect to the upstream pool.
    let mut upstream_stream = TcpStream::connect(&upstream_addr).await?;
    println!("Connected to upstream at {}", upstream_addr);

    let configure_message = json!({
        "id": 1,
        "method": "mining.configure",
        "params": [["version-rolling"], {"version-rolling.mask": "1fffe000"}]
    })
    .to_string();
    upstream_stream.write_all(configure_message.as_bytes()).await?;
    upstream_stream.write_all(b"\n").await?;
    upstream_stream.flush().await?;
    println!("Sent configure request to upstream.");

    // Send subscribe request to upstream.
    let subscribe_message = json!({
        "id": 2,
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
        "id": 3,
        "method": "mining.authorize",
        "params": [worker_name, "x"]  // Replace "x" with a password if needed.
    })
    .to_string();
    upstream_stream.write_all(authorize_message.as_bytes()).await?;
    upstream_stream.write_all(b"\n").await?;
    upstream_stream.flush().await?;
    println!("Sent authorize request to upstream.");

    // Send suggest_difficulty request to upstream.
    let suggest_difficulty_message = json!({
        "id": 4,
        "method": "mining.suggest_difficulty",
        "params": [1000.0]  // Replace with the desired difficulty
    })
    .to_string();
    upstream_stream.write_all(suggest_difficulty_message.as_bytes()).await?;
    upstream_stream.write_all(b"\n").await?;
    upstream_stream.flush().await?;
    println!("Sent suggest_difficulty request to upstream.");

    // Split the upstream connection.
    let (upstream_reader, upstream_writer) = upstream_stream.into_split();

    let job_params: SharedJobParams = Arc::new(Mutex::new(None));
    let last_notify: SharedLastNotify = Arc::new(Mutex::new(None));
    let last_difficulty: SharedLastNotify = Arc::new(Mutex::new(None));
    let last_prev_hash: SharedLastPrevHash = Arc::new(Mutex::new(None));

    // Broadcast channel for job (mining.notify) messages.
    let (job_tx, _) = broadcast::channel::<String>(16);
    // MPSC channel for share submissions forwarded upstream.
    let (share_tx, share_rx) = mpsc::channel::<String>(16);

    // Spawn task to read from upstream.
    {
        let job_tx = job_tx.clone();
        let job_params = job_params.clone();
        let last_notify = last_notify.clone();
        let last_difficulty = last_difficulty.clone();
        let last_prev_hash = last_prev_hash.clone();
        tokio::spawn(async move {
            upstream_read_handler(upstream_reader, job_tx, job_params, last_notify, last_difficulty, on_new_block, last_prev_hash).await;
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
        let last_difficulty = last_difficulty.clone();
        let worker_name = worker_name.to_string();
        let on_share_submitted = on_share_submitted.clone();
        tokio::spawn(async move {
            handle_miner(miner_socket, job_tx.subscribe(), share_tx, job_params, last_notify, last_difficulty, worker_name, on_share_submitted).await;
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
    last_difficutly: SharedLastDifficulty,
    on_new_block: Arc<dyn Fn(String) + Send + Sync + 'static>,
    last_prev_hash: SharedLastPrevHash,
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
                            if let Some(params) = val.get("params").and_then(|p| p.as_array()) {
                                if let Some(prevhash_json) = params.get(1) {
                                    let new_prevhash = prevhash_json.as_str().unwrap_or("").to_string();


                                    let mut last_hash_guard = last_prev_hash.lock().await;
                                    let changed = match &*last_hash_guard {
                                        Some(old_hash) => *old_hash != new_prevhash,
                                        None           => true,
                                    };

                                    if changed {
                                        *last_hash_guard = Some(new_prevhash);
                                        on_new_block(trimmed.to_string());
                                    }
                                }
                            }
                            {
                                // Cache the most recent notify.
                                let mut last_lock = last_notify.lock().await;
                                *last_lock = Some(trimmed.to_string());
                            }
                            let _ = job_tx.send(trimmed.to_string());
                            continue;
                        } else if method == "mining.set_difficulty" {
                            // Handle mining.set_difficulty
                            if let Some(params) = val.get("params").and_then(|p| p.as_array()) {
                                if let Some(difficulty) = params.get(0).and_then(|d| d.as_f64()) {
                                    let mut lock = job_params.lock().await;
                                    if let Some(ref mut job_params) = *lock {
                                        job_params.difficulty = difficulty;
                                        println!("Stored upstream difficulty: {}", difficulty);
                                    }
                                    drop(lock);
                                    // Cache the difficulty
                                    let mut last_lock = last_difficutly.lock().await;
                                    *last_lock = Some(trimmed.to_string());
                                    // Broadcast the set_difficulty message downstream
                                    let _ = job_tx.send(trimmed.to_string());
                                }
                            }
                            continue;
                        }
                    }
                    // Check for subscribe (id==1) response.
                    if let Some(id) = val.get("id").and_then(|v| v.as_u64()) {
                        if id == 2 {
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
                                            difficulty: 1000.0, // Default difficulty
                                        };
                                        let mut lock = job_params.lock().await;
                                        *lock = Some(params);
                                        println!("Stored upstream job parameters: {:?}", *lock);
                                    }
                                }
                            }
                        } else if id == 3 {
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
    last_difficulty: SharedLastDifficulty,
    worker_name: String,
    on_share_submitted: Arc<dyn Fn(String) + Send + Sync + 'static>,
) {
    let miner_id = MINER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let constrained_extranonce = format!("{:04x}", miner_id);
    println!("Assigned constrained extranonce {} to miner {}", constrained_extranonce, miner_id);

    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    // Read the miner’s configure request.
    if let Ok(n) = reader.read_line(&mut line).await {
        if n > 0 {
            println!("Miner {} sent: {}", miner_id, line.trim_end());

            // Build and send the configure response.
            let configure_response = json!({
                "id": 1,
                "result": {
                    "version-rolling": true,
                    "version-rolling.mask": "1fffe000",
                },
                "error": null
            });
            let response_str = serde_json::to_string(&configure_response).unwrap();
            if let Err(e) = writer.write_all(response_str.as_bytes()).await {
                println!("Error writing configure response to miner {}: {:?}", miner_id, e);
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
            println!("Response to Miner {}: {}", miner_id, response_str);
            line.clear();
        }
    } else {
        println!("Failed to read configure request from miner {}", miner_id);
        return;
    }

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
                "id": 2,
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
            println!("Response to Miner {}: {}", miner_id, response_str);
            line.clear();
        }
    } else {
        println!("Failed to read subscribe request from miner {}", miner_id);
        return;
    }

    // Read the miner’s authorize request.
    if let Ok(n) = reader.read_line(&mut line).await {
        if n > 0 {
            println!("Miner {} sent: {}", miner_id, line.trim_end());

            // Build and send the authorize response.
            let authorize_response = json!({
                "id": 3,
                "result": true,
                "error": null
            });
            let response_str = serde_json::to_string(&authorize_response).unwrap();
            if let Err(e) = writer.write_all(response_str.as_bytes()).await {
                println!("Error writing authorize response to miner {}: {:?}", miner_id, e);
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
            println!("Response to Miner {}: {}", miner_id, response_str);
            line.clear();

        }
    } else {
        println!("Failed to read authorize request from miner {}", miner_id);
        return;
    }

    // Immediately send the current mining.notify and mining.set_difficulty (if cached) after authorize.
    let cached_difficulty = {
        let lock = last_difficulty.lock().await;
        lock.clone()
    };
    if let Some(difficulty_msg) = cached_difficulty {
        println!("Sending cached mining.set_difficulty to miner {}: {}", miner_id, difficulty_msg);
        if let Err(e) = writer.write_all(difficulty_msg.as_bytes()).await {
            println!("Error sending difficulty to miner {}: {:?}", miner_id, e);
        }
        if let Err(e) = writer.write_all(b"\n").await {
            println!("Error writing newline to miner {}: {:?}", miner_id, e);
        }
    }
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

    // Process job notifications and miner messages concurrently.
    loop {
        tokio::select! {
            Ok(job_msg) = job_rx.recv() => {
                if let Err(e) = writer.write_all(job_msg.as_bytes()).await {
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
                        // Check if the line is a share submission message
                        if let Ok(value) = serde_json::from_str::<Value>(&line) {
                            if let Some(method) = value.get("method").and_then(|m| m.as_str()) {
                                if method == "mining.submit" {
                                    on_share_submitted(line.trim().to_string());

                                    let transformed_share = transform_share_submission(
                                        &line,
                                        &constrained_extranonce,
                                        &upstream_params.full_extranonce1,
                                        &worker_name
                                    );
                                    if let Err(e) = share_tx.send(transformed_share).await {
                                        println!("Error sending share from miner {} upstream: {:?}", miner_id, e);
                                        break;
                                    }
                                    // Respond to the miner with the submission result.
                                    if let Some(id) = value.get("id") {
                                        let submission_response = json!({
                                            "id": id,
                                            "result": true,
                                            "error": null
                                        });
                                        let response_str = serde_json::to_string(&submission_response).unwrap();
                                        if let Err(e) = writer.write_all(response_str.as_bytes()).await {
                                            println!("Error writing submission response to miner {}: {:?}", miner_id, e);
                                            break;
                                        }
                                        if let Err(e) = writer.write_all(b"\n").await {
                                            println!("Error writing newline to miner {}: {:?}", miner_id, e);
                                            break;
                                        }
                                        if let Err(e) = writer.flush().await {
                                            println!("Error flushing writer for miner {}: {:?}", miner_id, e);
                                            break;
                                        }
                                        println!("Response to Miner {}: {}", miner_id, response_str);
                                    }
                                }
                            }
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

/// Transforms a miner’s share submission by replacing the constrained extranonce with the full upstream extranonce.
fn transform_share_submission(submission: &str, constrained_extranonce: &str, full_extranonce: &str, worker_name: &str) -> String {
    if let Ok(mut value) = serde_json::from_str::<Value>(submission) {
        if let Some(method) = value.get("method").and_then(|m| m.as_str()) {
            if method == "mining.submit" {
                if let Some(params) = value.get_mut("params").and_then(|p| p.as_array_mut()) {
                    if params.len() >= 2 {
                        params[0] = Value::String(worker_name.to_string());
                        if let Some(extranonce_2) = params.get(2).and_then(|v| v.as_str()) {
                            params[2] = Value::String(constrained_extranonce.to_string() + extranonce_2);
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
