use std::env;
use tempfile::TempDir;

use nkv::srv;

const DEFAULT_URL: &str = "127.0.0.1:4222";

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = env::args().collect();

    let url = if args.len() > 1 {
        &args[1]
    } else {
        DEFAULT_URL
    };

    let temp_dir = TempDir::new().expect("Failed to create temporary directory");
    println!(
        "Server created with temproary dir being at {}",
        temp_dir.path().display()
    );

    // creates a task where it waits to serve threads
    let (mut srv, _cancel) = srv::Server::new(url.to_string(), temp_dir.path().to_path_buf())
        .await
        .unwrap();

    srv.serve().await;
}
