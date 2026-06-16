use anyhow::{Context, Result};
use dotenv::dotenv;
use futures::StreamExt;
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressStyle};
use object_store::{aws::AmazonS3Builder, path::Path as ObjectPath, ObjectStore};
use std::sync::Arc;
use tokio::{fs, sync::Semaphore};
use walkdir::WalkDir;

// Constants for large file handling
const MULTIPART_THRESHOLD: u64 = 50 * 1024 * 1024; // 50MB

#[derive(Debug, Clone)]
struct Config {
    minio_endpoint: String,
    minio_bucket: String,
    minio_access_key: String,
    minio_secret_key: String,
    source_dir: String,
    concurrent_uploads: usize,
    enable_checksum: bool,
}

impl Config {
    fn from_env() -> Result<Self> {
        Ok(Config {
            minio_endpoint: std::env::var("MINIO_ENDPOINT")
                .unwrap_or_else(|_| "http://localhost:9000".to_string()),
            minio_bucket: std::env::var("MINIO_BUCKET")
                .unwrap_or_else(|_| "sentinel-data-large".to_string()),
            minio_access_key: std::env::var("MINIO_ACCESS_KEY")
                .unwrap_or_else(|_| "minioadmin".to_string()),
            minio_secret_key: std::env::var("MINIO_SECRET_KEY")
                .unwrap_or_else(|_| "minioadmin".to_string()),
            source_dir: std::env::var("SOURCE_DIR")
                .unwrap_or_else(|_| "./test-data-large".to_string()),
            concurrent_uploads: std::env::var("CONCURRENT_UPLOADS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
            enable_checksum: std::env::var("ENABLE_CHECKSUM")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
        })
    }
}

async fn upload_large_file_with_progress(
    store: Arc<dyn ObjectStore>,
    data: Vec<u8>,
    object_path: &ObjectPath,
    pb: &ProgressBar,
) -> Result<()> {
    let size = data.len() as u64;

    if size > MULTIPART_THRESHOLD {
        // For large files, use multipart upload
        use bytes::Bytes;
        use futures::stream::{self, StreamExt};
        use object_store::PutPayload;

        let chunk_size = 10 * 1024 * 1024; // 10MB chunks
        let chunks: Vec<Bytes> = data
            .chunks(chunk_size)
            .map(|chunk| Bytes::copy_from_slice(chunk))
            .collect();

        let total_chunks = chunks.len();
        println!(
            "   📦 Multipart upload: {} chunks ({} MB each)",
            total_chunks,
            chunk_size / (1024 * 1024)
        );

        // Create a stream of chunks wrapped in PutPayload
        let stream = stream::iter(chunks).map(|chunk| {
            // Convert Bytes to PutPayload::Bytes
            PutPayload::from(chunk)
        });

        // Use put_multipart with stream
        let mut upload = store
            .put_multipart(object_path)
            .await
            .context("Failed to start multipart upload")?;

        // Send chunks one by one
        let mut chunk_idx = 0;
        futures::pin_mut!(stream);
        while let Some(payload) = stream.next().await {
            upload
                .put_part(payload)
                .await
                .with_context(|| format!("Failed to upload chunk {}", chunk_idx))?;
            chunk_idx += 1;

            // Update progress every 5 chunks
            if chunk_idx % 5 == 0 {
                pb.inc(1);
            }
        }

        // Complete the upload
        upload
            .complete()
            .await
            .context("Failed to complete multipart upload")?;

        // Ensure progress bar reflects completion
        pb.inc(1);
    } else {
        // Small file: single upload
        store
            .put(object_path, data.into())
            .await
            .context("Failed to upload small file")?;
        pb.inc(1);
    }

    Ok(())
}

async fn upload_file_with_checksum(
    store: Arc<dyn ObjectStore>,
    source_dir: &str,
    file_path: &str,
    pb: &ProgressBar,
    enable_checksum: bool,
) -> Result<u64> {
    let full_path = format!("{}/{}", source_dir, file_path);
    let metadata = fs::metadata(&full_path).await?;
    let file_size = metadata.len();

    // Read the file
    let data = fs::read(&full_path)
        .await
        .with_context(|| format!("Failed to read file: {}", full_path))?;

    let object_path = ObjectPath::from(file_path);

    // Calculate checksum if enabled
    if enable_checksum {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&data);
        let checksum = hex::encode(hasher.finalize());

        // Log checksum
        println!("   🔐 {}: {}...", file_path, &checksum[..8]);
    }

    // Upload with progress tracking
    upload_large_file_with_progress(store, data, &object_path, pb).await?;

    Ok(file_size)
}

async fn collect_files_with_size(dir: &str) -> Result<Vec<(String, u64)>> {
    let mut files = Vec::new();

    for entry in WalkDir::new(dir)
        .follow_links(true)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path();
        if path.is_file() {
            let rel_path = path
                .strip_prefix(dir)
                .unwrap_or(path)
                .to_str()
                .unwrap_or("")
                .to_string();

            let clean_path = path_clean::clean(rel_path);
            let size = tokio::fs::metadata(path).await?.len();
            files.push((clean_path.to_string_lossy().to_string(), size));
        }
    }

    Ok(files)
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    env_logger::init();

    let config = Config::from_env()?;

    println!("🚀 Starting LARGE Sentinel-2 migration to MinIO");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📁 Source: {}", config.source_dir);
    println!("🎯 Target: {}", config.minio_endpoint);
    println!("📦 Bucket: {}", config.minio_bucket);
    println!("🔄 Concurrent uploads: {}", config.concurrent_uploads);
    println!(
        "🔐 Checksum: {}",
        if config.enable_checksum {
            "enabled"
        } else {
            "disabled"
        }
    );
    println!(
        "📏 Multipart threshold: {} MB",
        MULTIPART_THRESHOLD / (1024 * 1024)
    );
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Setup MinIO client
    let client = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(&config.minio_endpoint)
            .with_bucket_name(&config.minio_bucket)
            .with_access_key_id(&config.minio_access_key)
            .with_secret_access_key(&config.minio_secret_key)
            .with_allow_http(true)
            .build()
            .context("Failed to build MinIO client")?,
    );

    // Test connection - simplified: just check if we can connect
    println!("🔗 Testing connection...");

    // Try to get the bucket info by listing objects (just one)
    let list_result: Result<_, object_store::Error> = Ok(client.list(None));

    match list_result {
        Ok(stream) => {
            // We have a connection, pin the stream and check if it has items
            let mut pinned_stream = std::pin::pin!(stream);
            match pinned_stream.next().await {
                Some(Ok(obj)) => {
                    println!("✅ Connected to MinIO successfully");
                    println!("   📋 Found existing object: {}", obj.location);
                }
                Some(Err(e)) => {
                    println!("⚠️  Connected but error reading objects: {}", e);
                    println!("   This might be because the bucket doesn't exist yet.");
                    println!("   Continuing anyway (will create objects on upload)...");
                }
                None => {
                    println!("✅ Connected to MinIO successfully (bucket is empty)");
                }
            }
        }
        Err(e) => {
            println!("❌ Failed to connect to MinIO: {}", e);
            println!("   Please check:");
            println!("   - MinIO is running: docker-compose ps");
            println!("   - Endpoint: {}", config.minio_endpoint);
            println!("   - Bucket exists: {}", config.minio_bucket);
            println!("   - Credentials are correct");
            println!("\n   Continuing anyway (will retry on upload)...");
        }
    }

    // Collect files with sizes
    println!("\n📂 Scanning for files...");
    let files = collect_files_with_size(&config.source_dir).await?;
    let total_files = files.len();
    let total_size: u64 = files.iter().map(|(_, size)| size).sum();

    println!(
        "✅ Found {} files ({})",
        total_files,
        HumanBytes(total_size)
    );

    if total_files == 0 {
        println!("⚠️  No files found. Nothing to do.");
        return Ok(());
    }

    // Setup multi-progress for better visibility
    let multi_pb = MultiProgress::new();
    let main_pb = multi_pb.add(ProgressBar::new(total_files as u64));
    main_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} files ({eta})")
            .unwrap()
            .progress_chars("#>-")
    );

    // Add a sub-progress for bytes
    let bytes_pb = multi_pb.add(ProgressBar::new(total_size));
    bytes_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.yellow} 📊 [{bar:40.green/blue}] {bytes}/{total_bytes} ({bytes_per_sec})")
            .unwrap()
            .progress_chars("▓▒░")
    );
    bytes_pb.set_prefix("Uploading");

    // Upload with concurrency
    let semaphore = Arc::new(Semaphore::new(config.concurrent_uploads));
    let mut handles = Vec::new();
    let start_time = std::time::Instant::now();

    println!("\n⬆️  Starting uploads...");

    for (file_path, _file_size) in files {
        let permit = semaphore.clone().acquire_owned().await?;
        let client_clone = client.clone();
        let source_dir_clone = config.source_dir.clone();
        let main_pb_clone = main_pb.clone();
        let bytes_pb_clone = bytes_pb.clone();
        let enable_checksum = config.enable_checksum;

        let handle = tokio::spawn(async move {
            let result = upload_file_with_checksum(
                client_clone,
                &source_dir_clone,
                &file_path,
                &main_pb_clone,
                enable_checksum,
            )
            .await;

            if let Ok(size) = result {
                bytes_pb_clone.inc(size);
            }

            drop(permit);
            result
        });

        handles.push(handle);
    }

    // Wait for all uploads
    let mut successful = 0;
    let mut failed = 0;
    let mut total_uploaded = 0u64;

    for handle in handles {
        match handle.await {
            Ok(Ok(size)) => {
                successful += 1;
                total_uploaded += size;
            }
            Ok(Err(e)) => {
                eprintln!("❌ Upload failed: {}", e);
                failed += 1;
            }
            Err(e) => {
                eprintln!("❌ Task panicked: {}", e);
                failed += 1;
            }
        }
    }

    let elapsed = start_time.elapsed();
    main_pb.finish_with_message("✅ Migration complete!");
    bytes_pb.finish();

    // Summary
    println!("\n📊 Migration Summary");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("✅ Successful: {}", successful);
    println!("❌ Failed: {}", failed);
    println!("📁 Total files: {}", successful + failed);
    println!("💾 Data uploaded: {}", HumanBytes(total_uploaded));
    println!("⏱️  Time elapsed: {:.2}s", elapsed.as_secs_f32());

    if successful > 0 && elapsed.as_secs_f32() > 0.0 {
        let speed = total_uploaded as f64 / elapsed.as_secs_f64();
        println!("🚀 Average speed: {:.2} MB/s", speed / (1024.0 * 1024.0));
    }

    println!("\n🔗 MinIO Console: http://localhost:9001");
    println!("🔑 Login: minioadmin / minioadmin");

    Ok(())
}
