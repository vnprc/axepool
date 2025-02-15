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
    // These are typically provided by the pool in mining.notify messages.
    // For a real proxy you’d need to capture all coinbase‐related parts.
    coinb1: String,
    coinb2: String,
    // The full extranonce1 that the upstream pool uses.
    full_extranonce1: String,
    // The size (in bytes) of extranonce2.
    extranonce2_size: usize,
}

// Shared state to store upstream job parameters.
type SharedJobParams = Arc<Mutex<Option<JobParams>>>;

// An atomic counter to assign each miner a unique constrained extranonce.
static MINER_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Obtain the upstream pool address and worker name from the command line.
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

    // Now split the upstream connection into read and write halves.
    let (upstream_reader, upstream_writer) = upstream_stream.into_split();

    // Shared job parameters from upstream.
    let job_params: SharedJobParams = Arc::new(Mutex::new(None));

    // Broadcast channel for job (mining.notify) messages from upstream.
    let (job_tx, _) = broadcast::channel::<String>(16);

    // MPSC channel for share submissions that need to be forwarded upstream.
    let (share_tx, share_rx) = mpsc::channel::<String>(16);

    // Spawn a task to read messages from upstream.
    {
        let job_tx = job_tx.clone();
        let job_params = job_params.clone();
        tokio::spawn(async move {
            upstream_read_handler(upstream_reader, job_tx, job_params).await;
        });
    }

    // Spawn a task to write share submissions to upstream.
    {
        tokio::spawn(async move {
            upstream_write_handler(upstream_writer, share_rx).await;
        });
    }

    // Listen for connections from downstream miners.
    let local_addr = "0.0.0.0:3334"; // Use a port separate from upstream.
    let listener = TcpListener::bind(local_addr).await?;
    println!("Listening for miners on {}", local_addr);

    loop {
        let (miner_socket, addr) = listener.accept().await?;
        println!("Accepted miner connection from {}", addr);
        let job_tx = job_tx.clone();
        let share_tx = share_tx.clone();
        let job_params = job_params.clone();
        tokio::spawn(async move {
            handle_miner(miner_socket, job_tx.subscribe(), share_tx, job_params).await;
        });
    }
}

/// Reads messages from the upstream pool. In this demo:
/// - If the message is a subscribe response, we extract the full extranonce1
///   and extranonce2 size and store them in shared state.
/// - If the message is a mining.notify, we broadcast it to all miners.
async fn upstream_read_handler(
    reader: tokio::net::tcp::OwnedReadHalf,
    job_tx: broadcast::Sender<String>,
    job_params: SharedJobParams,
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
                println!("Upstream message: {}", trimmed);
                if trimmed.is_empty() {
                    continue;
                }
                if let Ok(val) = serde_json::from_str::<Value>(trimmed) {
                    // Handle the JSON message as before.
                    if let Some(method) = val.get("method").and_then(|m| m.as_str()) {
                        if method == "mining.notify" {
                            let _ = job_tx.send(trimmed.to_string());
                            continue;
                        }
                    }
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


/// Writes share submissions received from miners to the upstream pool.
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

/// Handles communication with a single miner:
/// - Reads the miner’s subscribe request and replies with a subscribe response
///   that uses a constrained extranonce (e.g. 2 bytes).
/// - Listens for upstream job notifications, modifies them for this miner,
///   and forwards them.
/// - Reads share submissions from the miner, transforms them (replacing the
///   constrained extranonce with the upstream full extranonce), and sends them upstream.
async fn handle_miner(
    socket: TcpStream,
    mut job_rx: broadcast::Receiver<String>,
    share_tx: mpsc::Sender<String>,
    job_params: SharedJobParams,
) {
    // Assign a unique constrained extranonce (e.g. 2 bytes in hex, i.e. 4 hex digits).
    let miner_id = MINER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let constrained_extranonce = format!("{:04x}", miner_id);
    println!(
        "Assigned constrained extranonce {} to miner {}",
        constrained_extranonce, miner_id
    );

    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    let mut subscribed = false;

    loop {
        tokio::select! {
            // Branch for sending job notifications from upstream.
            Ok(job_msg) = job_rx.recv() => {
                // Only send notify messages if the miner has subscribed.
                if subscribed {
                    let modified_job = modify_job_for_miner(&job_msg, &constrained_extranonce);
                    if let Err(e) = writer.write_all(modified_job.as_bytes()).await {
                        println!("Error sending job to miner {}: {:?}", miner_id, e);
                        break;
                    }
                    if let Err(e) = writer.write_all(b"\n").await {
                        println!("Error sending newline to miner {}: {:?}", miner_id, e);
                        break;
                    }
                }
            }
            // Branch for reading messages from the miner.
            result = reader.read_line(&mut line) => {
                match result {
                    Ok(0) => {
                        println!("Miner {} disconnected", miner_id);
                        break;
                    }
                    Ok(_) => {
                        println!("Received from miner {}: {}", miner_id, line.trim_end());
                        // Parse the JSON message.
                        if let Ok(val) = serde_json::from_str::<Value>(&line) {
                            if let Some(method) = val.get("method").and_then(|m| m.as_str()) {
                                match method {
                                    "mining.configure" => {
                                        // Extract the id if present.
                                        let id = val.get("id").cloned().unwrap_or(Value::Null);
                                        // Respond with the full version-rolling mask.
                                        let configure_response = json!({
                                            "id": id,
                                            "result": { "version-rolling.mask": "ffffffff" },
                                            "error": null
                                        });
                                        let resp_str = serde_json::to_string(&configure_response).unwrap();
                                        if let Err(e) = writer.write_all(resp_str.as_bytes()).await {
                                            println!("Error sending mining.configure response to miner {}: {:?}", miner_id, e);
                                        }
                                        if let Err(e) = writer.write_all(b"\n").await {
                                            println!("Error sending newline to miner {}: {:?}", miner_id, e);
                                        }
                                        if let Err(e) = writer.flush().await {
                                            println!("Error flushing writer for miner {}: {:?}", miner_id, e);
                                        }
                                    },
                                    "mining.subscribe" => {
                                        // Only handle subscription once.
                                        if !subscribed {
                                            // Wait for upstream job parameters.
                                            let upstream_params = loop {
                                                let lock = job_params.lock().await;
                                                if let Some(ref params) = *lock {
                                                    break params.clone();
                                                }
                                                drop(lock);
                                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                                            };
                                            // Build the subscribe response using the constrained extranonce.
                                            let subscribe_response = json!({
                                                "id": val.get("id").cloned().unwrap_or(Value::Null),
                                                "result": [
                                                    [
                                                        ["mining.set_difficulty", "1"],
                                                        ["mining.notify", "1"]
                                                    ],
                                                    constrained_extranonce, // Constrained extranonce1.
                                                    upstream_params.extranonce2_size
                                                ],
                                                "error": null
                                            });
                                            let response_str = serde_json::to_string(&subscribe_response).unwrap();
                                            if let Err(e) = writer.write_all(response_str.as_bytes()).await {
                                                println!("Error writing subscribe response to miner {}: {:?}", miner_id, e);
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
                                            println!("Sent subscribe response to miner {}", miner_id);
                                            subscribed = true;
                                        }
                                    },
                                    "mining.submit" => {
                                        // Assume this is a share submission.
                                        let upstream_params = {
                                            let lock = job_params.lock().await;
                                            if let Some(ref params) = *lock {
                                                params.clone()
                                            } else {
                                                println!("No upstream parameters available for miner {}", miner_id);
                                                continue;
                                            }
                                        };
                                        let transformed_share = transform_share_submission(&line, &constrained_extranonce, &upstream_params.full_extranonce1);
                                        if let Err(e) = share_tx.send(transformed_share).await {
                                            println!("Error sending share from miner {} upstream: {:?}", miner_id, e);
                                            break;
                                        }
                                    },
                                    _ => {
                                        println!("Unhandled method {} from miner {}", method, miner_id);
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


/// Modifies an upstream job (mining.notify) for a miner by injecting the miner’s
/// constrained extranonce into the coinbase.
/// (Note: A production implementation must reassemble the coinbase and merkle branch properly.)
fn modify_job_for_miner(job: &str, constrained_extranonce: &str) -> String {
    if let Ok(mut value) = serde_json::from_str::<Value>(job) {
        if let Some(method) = value.get("method").and_then(|m| m.as_str()) {
            if method == "mining.notify" {
                if let Some(params) = value.get_mut("params").and_then(|p| p.as_array_mut()) {
                    // For demonstration, assume params[1] is coinb1 and params[2] is coinb2.
                    if params.len() >= 3 {
                        if let (Some(coinb1), Some(coinb2)) =
                            (params.get(1).and_then(|v| v.as_str()), params.get(2).and_then(|v| v.as_str()))
                        {
                            // Insert the constrained_extranonce between coinb1 and coinb2.
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

/// Transforms a miner’s share submission (mining.submit) by replacing the constrained extranonce
/// with the upstream full extranonce. (In a real proxy you would need to reconstruct the coinbase.)
fn transform_share_submission(submission: &str, constrained_extranonce: &str, full_extranonce: &str) -> String {
    if let Ok(mut value) = serde_json::from_str::<Value>(submission) {
        if let Some(method) = value.get("method").and_then(|m| m.as_str()) {
            if method == "mining.submit" {
                if let Some(params) = value.get_mut("params").and_then(|p| p.as_array_mut()) {
                    // For demonstration, assume that the job id (params[1]) encodes the extranonce.
                    if params.len() >= 2 {
                        if let Some(job_id) = params.get(1).and_then(|v| v.as_str()) {
                            // Replace the constrained extranonce with the full extranonce.
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
