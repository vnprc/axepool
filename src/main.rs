use sv1_proxy::run_proxy;
use tokio::runtime::Runtime;

fn main() {
    if let Err(e) = run() {
        eprintln!("Error: {:#}", e);
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let rt = Runtime::new()?;

    let args: Vec<String> = std::env::args().collect();
    let upstream_addr = args.get(1).expect("Missing upstream address argument").clone();
    let worker_name   = args.get(2).expect("Missing worker name argument").clone();

    let on_new_block = Arc::new(|block_msg: String| {
        println!("(Callback) A new block arrived: {}", block_msg);
        // Possibly do more interesting things here
    });

    let on_share_submitted = Arc::new(|share_msg: String| {
        println!("(Callback) Miner submitted a share: {}", share_msg);
        // Possibly do more interesting things here
    });

    rt.block_on(async {
        run_proxy(
            &upstream_addr,
            &worker_name,
            on_new_block,
            on_share_submitted,
        ).await
    })
}
