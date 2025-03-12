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

    rt.block_on(async {
        run_proxy(
            &upstream_addr,
            &worker_name,
        ).await
    })
}
