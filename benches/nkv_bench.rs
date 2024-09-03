use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use nkv::nkv::NotifyKeyValue;
use nkv::srv::{BaseMsg, GetMsg, PutMsg, Server};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

fn bench_nkv(c: &mut Criterion) {
    let mut group = c.benchmark_group("nkv_group");

    // Create a Tokio runtime
    let rt = Runtime::new().unwrap();

    // Define different input sizes to test
    let input_sizes = [1024, 2048, 4096, 8192, 16384];

    group.bench_function(format!("nkv_new"), |b| {
        b.to_async(&rt).iter(|| async {
            let temp_dir = TempDir::new().unwrap();
            let result = NotifyKeyValue::new(temp_dir.path().to_path_buf());
            black_box(result)
        })
    });

    for &size in &input_sizes {
        group.bench_function(format!("nkv_single_put_size_{}", size), |b| {
            b.to_async(&rt).iter_batched(
                || {
                    // Setup code: create a Box<[u8]> with the given size
                    let data = vec![0u8; size].into_boxed_slice();
                    data
                },
                |input| async {
                    let temp_dir = TempDir::new().unwrap();
                    let mut nkv = NotifyKeyValue::new(temp_dir.path().to_path_buf()).unwrap();
                    let result = nkv.put("key1", input).await;
                    black_box(result)
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function(format!("nkv_single_update_size_{}", size), |b| {
            b.to_async(&rt).iter_batched(
                || {
                    // Setup code: create a Box<[u8]> with the given size
                    let data = vec![0u8; size].into_boxed_slice();
                    let new_data = vec![6u8; size].into_boxed_slice();
                    (data, new_data)
                },
                |(data, new_data)| async {
                    let temp_dir = TempDir::new().unwrap();
                    let mut nkv = NotifyKeyValue::new(temp_dir.path().to_path_buf()).unwrap();
                    nkv.put("key1", data).await;
                    let result = nkv.put("key1", new_data).await;
                    black_box(result)
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function(format!("nkv_single_get_size{}", size), |b| {
            b.iter_batched(
                || {
                    let data = vec![0u8; size].into_boxed_slice();
                    let temp_dir = TempDir::new().unwrap();
                    let mut nkv = NotifyKeyValue::new(temp_dir.path().to_path_buf()).unwrap();
                    let rt = Runtime::new().unwrap();
                    rt.block_on(nkv.put("key1", data));
                    nkv
                },
                |nkv| black_box(nkv.get("key1")),
                BatchSize::SmallInput, // Adjust based on expected input size
            );
        });

        group.bench_function(format!("nkv_single_delete_size{}", size), |b| {
            b.to_async(&rt).iter_batched(
                || {
                    let data = vec![0u8; size].into_boxed_slice();
                    data
                },
                |data| async {
                    let temp_dir = TempDir::new().unwrap();
                    let mut nkv = NotifyKeyValue::new(temp_dir.path().to_path_buf()).unwrap();

                    nkv.put("key1", data).await;
                    let result = nkv.delete("key1").await;
                    black_box(result)
                },
                BatchSize::SmallInput, // Adjust based on expected input size
            );
        });
    }

    group.finish();
}

fn bench_server(c: &mut Criterion) {
    let mut group = c.benchmark_group("server_group");

    let rt = Runtime::new().unwrap();
    group.bench_function(format!("nkv_new"), |b| {
        b.to_async(&rt).iter(|| async {
            let temp_dir = TempDir::new().expect("Failed to create temporary directory");
            let url = "127.0.0.1:8091";

            let result = Server::new(url.to_string(), temp_dir.path().to_path_buf()).await;
            black_box(result)
        })
    });

    let input_sizes = [1024, 2048, 4096, 8192, 16384];

    for &size in &input_sizes {
        group.bench_function(format!("server_put_size_{}", size), |b| {
            b.to_async(&rt).iter_batched(
                || {
                    // Setup code: create a Box<[u8]> with the given size
                    let data = vec![0u8; size].into_boxed_slice();
                    data
                },
                |input| async {
                    let temp_dir = TempDir::new().expect("Failed to create temporary directory");
                    // not used with channels
                    let url = "127.0.0.1:8091";

                    let (srv, _cancel) =
                        Server::new(url.to_string(), temp_dir.path().to_path_buf())
                            .await
                            .unwrap();

                    let put_tx = srv.put_tx();

                    let key = "key1".to_string();
                    let (resp_tx, mut resp_rx) = mpsc::channel(1);

                    let _ = put_tx.send(PutMsg {
                        key,
                        value: input,
                        resp_tx,
                    });

                    let result = resp_rx.recv().await;

                    black_box(result)
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function(format!("server_put_get_size_{}", size), |b| {
            b.to_async(&rt).iter_batched(
                || {
                    // Setup code: create a Box<[u8]> with the given size
                    let data = vec![0u8; size].into_boxed_slice();
                    data
                },
                |input| async {
                    let temp_dir = TempDir::new().expect("Failed to create temporary directory");
                    // not used with channels
                    let url = "127.0.0.1:8091";

                    let (srv, _cancel) =
                        Server::new(url.to_string(), temp_dir.path().to_path_buf())
                            .await
                            .unwrap();

                    let put_tx = srv.put_tx();
                    let get_tx = srv.get_tx();

                    let (resp_tx, mut resp_rx) = mpsc::channel(1);

                    let _ = put_tx.send(PutMsg {
                        key: "key1".to_string(),
                        value: input,
                        resp_tx,
                    });

                    let _ = resp_rx.recv().await.unwrap();

                    let (get_resp_tx, mut get_resp_rx) = mpsc::channel(1);
                    let _ = get_tx.send(GetMsg {
                        key: "key1".to_string(),
                        resp_tx: get_resp_tx,
                    });
                    let result = get_resp_rx.recv().await.unwrap();

                    black_box(result)
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function(format!("server_put_delete_size_{}", size), |b| {
            b.to_async(&rt).iter_batched(
                || {
                    // Setup code: create a Box<[u8]> with the given size
                    let data = vec![0u8; size].into_boxed_slice();
                    data
                },
                |input| async {
                    let temp_dir = TempDir::new().expect("Failed to create temporary directory");
                    // not used with channels
                    let url = "127.0.0.1:8091";

                    let (srv, _cancel) =
                        Server::new(url.to_string(), temp_dir.path().to_path_buf())
                            .await
                            .unwrap();

                    let put_tx = srv.put_tx();
                    let del_tx = srv.del_tx();

                    let (resp_tx, mut resp_rx) = mpsc::channel(1);

                    let _ = put_tx.send(PutMsg {
                        key: "key1".to_string(),
                        value: input,
                        resp_tx,
                    });

                    let _ = resp_rx.recv().await.unwrap();

                    let (del_resp_tx, mut del_resp_rx) = mpsc::channel(1);
                    let _ = del_tx.send(BaseMsg {
                        key: "key1".to_string(),
                        resp_tx: del_resp_tx,
                    });
                    let result = del_resp_rx.recv().await.unwrap();

                    black_box(result)
                },
                BatchSize::SmallInput,
            );
        });
    }
}

criterion_group!(benches, bench_nkv, bench_server);
criterion_main!(benches);
