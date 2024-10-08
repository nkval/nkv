// SPDX-License-Identifier: Apache-2.0

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use nkv::nkv::NkvStorage;
use nkv::notifier::TcpNotifier;
use nkv::persist_value::FileStorage;
use nkv::srv::{BaseMsg, GetMsg, PutMsg, Server};
use nkv::trie::Trie;
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
            let result =
                NkvStorage::<FileStorage, TcpNotifier>::new(temp_dir.path().to_path_buf()).unwrap();
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
                    let mut nkv =
                        NkvStorage::<FileStorage, TcpNotifier>::new(temp_dir.path().to_path_buf())
                            .unwrap();
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
                    let mut nkv =
                        NkvStorage::<FileStorage, TcpNotifier>::new(temp_dir.path().to_path_buf())
                            .unwrap();
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
                    let mut nkv =
                        NkvStorage::<FileStorage, TcpNotifier>::new(temp_dir.path().to_path_buf())
                            .unwrap();
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
                    let mut nkv =
                        NkvStorage::<FileStorage, TcpNotifier>::new(temp_dir.path().to_path_buf())
                            .unwrap();

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
                        uuid: "0".to_string(),
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

fn bench_trie(c: &mut Criterion) {
    let mut group = c.benchmark_group("trie_group");

    let input_sizes = [1024, 2048, 4096, 8192, 16384];

    for &size in &input_sizes {
        group.bench_function(format!("trie_insert_size_{}", size), |b| {
            b.iter_batched(
                || {
                    // Setup code: create a Box<[u8]> with the given size
                    let data = vec![0u8; size].into_boxed_slice();
                    let trie = Trie::new();
                    (data, trie)
                },
                |(input, mut trie)| {
                    let result = trie.insert("service.function.key1", input);
                    black_box(result)
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function(format!("trie_get_size_{}", size), |b| {
            b.iter_batched(
                || {
                    // Setup code: create a Box<[u8]> with the given size
                    let data = vec![0u8; size].into_boxed_slice();
                    let mut trie = Trie::new();
                    let _ = trie.insert("service.function.key1", data);
                    trie
                },
                |trie| {
                    let _ = trie.get("service.function.key1");
                    black_box(true)
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function(format!("trie_get_wildcard_{}", size), |b| {
            b.iter_batched(
                || {
                    let mut trie = Trie::new();
                    let val1 = vec![0u8; size].into_boxed_slice();
                    let val2 = vec![1u8; size].into_boxed_slice();
                    let val3 = vec![2u8; size].into_boxed_slice();
                    let val4 = vec![3u8; size].into_boxed_slice();
                    let val5 = vec![4u8; size].into_boxed_slice();

                    trie.insert("service.functionA.key1", val1);
                    trie.insert("service.functionA.key2", val2);
                    trie.insert("service.functionB.key1", val3);
                    trie.insert("service.functionB.key2", val4);
                    trie.insert("service.functionC.subfunction.key1", val5);
                    trie
                },
                |trie| {
                    let _ = trie.get("service.*");
                    black_box(true)
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function(format!("trie_remove_single_{}", size), |b| {
            b.iter_batched(
                || {
                    let mut trie = Trie::new();
                    let val1 = vec![0u8; size].into_boxed_slice();
                    let val2 = vec![1u8; size].into_boxed_slice();
                    let val3 = vec![2u8; size].into_boxed_slice();
                    let val4 = vec![3u8; size].into_boxed_slice();
                    let val5 = vec![4u8; size].into_boxed_slice();

                    trie.insert("service.functionA.key1", val1);
                    trie.insert("service.functionA.key2", val2);
                    trie.insert("service.functionB.key1", val3);
                    trie.insert("service.functionB.key2", val4);
                    trie.insert("service.functionC.subfunction.key1", val5);
                    trie
                },
                |mut trie| {
                    let result = trie.remove("service.functionA.key1");
                    black_box(result)
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function(format!("trie_remove_group_{}", size), |b| {
            b.iter_batched(
                || {
                    let mut trie = Trie::new();
                    let val1 = vec![0u8; size].into_boxed_slice();
                    let val2 = vec![1u8; size].into_boxed_slice();
                    let val3 = vec![2u8; size].into_boxed_slice();
                    let val4 = vec![3u8; size].into_boxed_slice();
                    let val5 = vec![4u8; size].into_boxed_slice();

                    trie.insert("service.functionA.key1", val1);
                    trie.insert("service.functionA.key2", val2);
                    trie.insert("service.functionB.key1", val3);
                    trie.insert("service.functionB.key2", val4);
                    trie.insert("service.functionC.subfunction.key1", val5);
                    trie
                },
                |mut trie| {
                    let result = trie.remove("service.functionB");
                    black_box(result)
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function(format!("trie_insert_overwrite_{}", size), |b| {
            b.iter_batched(
                || {
                    let trie = Trie::new();
                    let val1 = vec![0u8; size].into_boxed_slice();
                    let val2 = vec![1u8; size].into_boxed_slice();
                    (trie, val1, val2)
                },
                |(mut trie, val1, val2)| {
                    trie.insert("service.function.key1", val1);
                    let result = trie.insert("service.function.key1", val2);
                    black_box(result)
                },
                BatchSize::SmallInput,
            );
        });
    }
}

criterion_group!(benches, bench_nkv, bench_server, bench_trie);
criterion_main!(benches);
