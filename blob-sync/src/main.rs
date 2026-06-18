use anyhow::{Context, Result};
use chrono::NaiveDate;
use dotenv::dotenv;
use futures::StreamExt;
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressStyle};
use object_store::{aws::AmazonS3Builder, path::Path as ObjectPath, ObjectStore};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::{fs, sync::Semaphore};
use uuid::Uuid;
use walkdir::WalkDir;

mod db;

const MULTIPART_THRESHOLD: u64 = 50 * 1024 * 1024; // 50MB

fn rename_tif(original_name: &str) -> String {
    let name_without_ext = original_name.trim_end_matches(".tif");
    let parts: Vec<&str> = name_without_ext.split('_').collect();

    // Extract date (index 4: dharoi_dam_S2B_42QZM_20260210_0_L2A)
    let date_str = parts.get(4).unwrap_or(&"19700101");
    let date = NaiveDate::parse_from_str(date_str, "%Y%m%d")
        .unwrap_or_else(|_| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());

    // Dam name: first 2 parts (dharoi_dam)
    let dam_name = parts.get(0..2).unwrap_or(&["unknown"]).join("_");

    // Dataset name: satellite_grid_level (S2B_42QZM_L2A) - skip date and orbit
    let dataset_parts: Vec<&str> = parts
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != 4 && *i != 5) // skip date and orbit number
        .skip(2) // skip dam name parts
        .map(|(_, p)| *p)
        .collect();
    let dataset_name = dataset_parts.join("_");

    let uuid = Uuid::new_v4();
    format!(
        "{}_{}_{}_{}.tif",
        uuid,
        date.format("%Y%m%d"),
        dam_name,
        dataset_name
    )
}

fn extract_crs(file_path: &str) -> Option<String> {
    use std::fs::File;
    use tiff::decoder::Decoder;
    use tiff::tags::Tag;

    let file = File::open(file_path).ok()?;
    let mut decoder = Decoder::new(std::io::BufReader::new(file)).ok()?;

    if let Ok(val) = decoder.get_tag(Tag::GeoAsciiParamsTag) {
        if let tiff::decoder::ifd::Value::Ascii(s) = val {
            return Some(s.trim_end_matches('\0').to_string());
        }
    }

    if let Ok(val) = decoder.get_tag(Tag::GeoKeyDirectoryTag) {
        if let tiff::decoder::ifd::Value::List(items) = val {
            let keys: Vec<u16> = items
                .into_iter()
                .filter_map(|v| match v {
                    tiff::decoder::ifd::Value::Short(x) => Some(x),
                    _ => None,
                })
                .collect();
            if keys.len() >= 4 {
                let num_keys = keys[3] as usize;
                for i in 0..num_keys {
                    let offset = 4 + (i * 4);
                    if offset + 4 <= keys.len() {
                        let key_id = keys[offset];
                        if key_id == 1024 && keys[offset + 3] == 1 {
                            let model = match keys[offset + 2] {
                                1 => "ModelTypeProjected",
                                2 => "ModelTypeGeographic",
                                3 => "ModelTypeGeocentric",
                                _ => "Unknown",
                            };
                            return Some(format!("GeoTIFF ModelType: {}", model));
                        }
                        if key_id == 1026 {
                            return Some(format!(
                                "GeoTIFF Citation key present (tag {})",
                                keys[offset + 1]
                            ));
                        }
                    }
                }
            }
            return Some("GeoTIFF keys present (no WKT)".to_string());
        }
    }

    None
}

#[derive(Debug, Clone)]
struct Config {
    minio_endpoint: String,
    minio_bucket: String,
    minio_access_key: String,
    minio_secret_key: String,
    source_dir: String,
    concurrent_uploads: usize,
    enable_checksum: bool,
    database_url: String,
}

impl Config {
    fn from_env() -> Result<Self> {
        Ok(Config {
            minio_endpoint: std::env::var("MINIO_ENDPOINT")
                .unwrap_or_else(|_| "http://localhost:9000".to_string()),
            minio_bucket: std::env::var("MINIO_BUCKET")
                .unwrap_or_else(|_| "sentinel-2".to_string()),
            minio_access_key: std::env::var("MINIO_ACCESS_KEY")
                .unwrap_or_else(|_| "minioadmin".to_string()),
            minio_secret_key: std::env::var("MINIO_SECRET_KEY")
                .unwrap_or_else(|_| "minioadmin".to_string()),
            source_dir: std::env::var("SOURCE_DIR").unwrap_or_else(|_| "./raw_outputs".to_string()),
            concurrent_uploads: std::env::var("CONCURRENT_UPLOADS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
            enable_checksum: std::env::var("ENABLE_CHECKSUM")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            database_url: std::env::var("DATABASE_URL").unwrap_or_else(|_| {
                "postgres://blobuser:blobpass@localhost:5432/blob_sync".to_string()
            }),
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
        use bytes::Bytes;
        use futures::stream::{self, StreamExt};
        use object_store::PutPayload;

        let chunk_size = 10 * 1024 * 1024;
        let chunks: Vec<Bytes> = data
            .chunks(chunk_size)
            .map(|chunk| Bytes::copy_from_slice(chunk))
            .collect();

        let total_chunks = chunks.len();
        println!(
            "   Multipart upload: {} chunks ({} MB each)",
            total_chunks,
            chunk_size / (1024 * 1024)
        );

        let stream = stream::iter(chunks).map(PutPayload::from);

        let mut upload = store
            .put_multipart(object_path)
            .await
            .context("Failed to start multipart upload")?;

        let mut chunk_idx = 0;
        futures::pin_mut!(stream);
        while let Some(payload) = stream.next().await {
            upload
                .put_part(payload)
                .await
                .with_context(|| format!("Failed to upload chunk {}", chunk_idx))?;
            chunk_idx += 1;

            if chunk_idx % 5 == 0 {
                pb.inc(1);
            }
        }

        upload
            .complete()
            .await
            .context("Failed to complete multipart upload")?;

        pb.inc(1);
    } else {
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
) -> Result<(u64, String, Option<String>, Option<String>)> {
    let full_path = format!("{}/{}", source_dir, file_path);
    let metadata = fs::metadata(&full_path).await?;
    let file_size = metadata.len();

    let data = fs::read(&full_path)
        .await
        .with_context(|| format!("Failed to read file: {}", full_path))?;

    let original_name = file_path.split('/').last().unwrap_or(file_path).to_string();
    let renamed_name = rename_tif(&original_name);
    let raw_path = format!("raw/{}", renamed_name);
    let object_path = ObjectPath::from(raw_path.as_str());

    let mut checksum = None;
    if enable_checksum {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&data);
        let hash = hex::encode(hasher.finalize());
        println!("   {}: {}...", original_name, &hash[..8]);
        checksum = Some(hash);
    }

    let crs = extract_crs(&full_path);

    upload_large_file_with_progress(store, data, &object_path, pb).await?;

    Ok((file_size, renamed_name, checksum, crs))
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
            let ext = path.extension().and_then(|e| e.to_str());
            if ext != Some("tif") {
                continue;
            }

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

    println!("Starting Sentinel-2 migration to MinIO");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Source: {}", config.source_dir);
    println!("Target: {}", config.minio_endpoint);
    println!("Bucket: {}", config.minio_bucket);
    println!("Concurrent uploads: {}", config.concurrent_uploads);
    println!(
        "Checksum: {}",
        if config.enable_checksum {
            "enabled"
        } else {
            "disabled"
        }
    );
    println!(
        "Multipart threshold: {} MB",
        MULTIPART_THRESHOLD / (1024 * 1024)
    );
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    println!("Connecting to Postgres...");
    let pool = PgPool::connect(&config.database_url)
        .await
        .context("Failed to connect to Postgres")?;
    println!("Connected to Postgres");

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

    println!("Testing MinIO connection...");

    let list_result: Result<_, object_store::Error> = Ok(client.list(None));

    match list_result {
        Ok(stream) => {
            let mut pinned_stream = std::pin::pin!(stream);
            match pinned_stream.next().await {
                Some(Ok(obj)) => {
                    println!("Connected to MinIO successfully");
                    println!("  Found existing object: {}", obj.location);
                }
                Some(Err(e)) => {
                    println!("  Connected but error reading objects: {}", e);
                    println!("  Continuing anyway (will create objects on upload)...");
                }
                None => {
                    println!("Connected to MinIO successfully (bucket is empty)");
                }
            }
        }
        Err(e) => {
            println!("Failed to connect to MinIO: {}", e);
            println!("  Please check:");
            println!("  - MinIO is running: docker-compose ps");
            println!("  - Endpoint: {}", config.minio_endpoint);
            println!("  - Bucket exists: {}", config.minio_bucket);
            println!("  - Credentials are correct");
            println!("\n  Continuing anyway (will retry on upload)...");
        }
    }

    println!("\nScanning for files...");
    let files = collect_files_with_size(&config.source_dir).await?;
    let total_files = files.len();
    let total_size: u64 = files.iter().map(|(_, size)| size).sum();

    println!("Found {} files ({})", total_files, HumanBytes(total_size));

    if total_files == 0 {
        println!("No files found. Nothing to do.");
        return Ok(());
    }

    let multi_pb = MultiProgress::new();
    let main_pb = multi_pb.add(ProgressBar::new(total_files as u64));
    main_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} files ({eta})")
            .unwrap()
            .progress_chars("#>-")
    );

    let bytes_pb = multi_pb.add(ProgressBar::new(total_size));
    bytes_pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.yellow} [{bar:40.green/blue}] {bytes}/{total_bytes} ({bytes_per_sec})",
            )
            .unwrap()
            .progress_chars("▓▒░"),
    );
    bytes_pb.set_prefix("Uploading");

    let semaphore = Arc::new(Semaphore::new(config.concurrent_uploads));
    let mut handles = Vec::new();
    let start_time = std::time::Instant::now();

    println!("\nStarting uploads...");

    for (file_path, _file_size) in files {
        let permit = semaphore.clone().acquire_owned().await?;
        let client_clone = client.clone();
        let source_dir_clone = config.source_dir.clone();
        let main_pb_clone = main_pb.clone();
        let bytes_pb_clone = bytes_pb.clone();
        let enable_checksum = config.enable_checksum;
        let pool_clone = pool.clone();
        let bucket_name = config.minio_bucket.clone();

        let handle = tokio::spawn(async move {
            let result = upload_file_with_checksum(
                client_clone,
                &source_dir_clone,
                &file_path,
                &main_pb_clone,
                enable_checksum,
            )
            .await;

            if let Ok((size, ref renamed_name, ref checksum, ref crs)) = result {
                bytes_pb_clone.inc(size);

                let original_name = file_path
                    .split('/')
                    .last()
                    .unwrap_or(&file_path)
                    .to_string();

                let file_url =
                    format!("http://localhost:9000/{}/raw/{}", bucket_name, renamed_name);

                let record = db::FileRecord {
                    original_name,
                    renamed_name: renamed_name.clone(),
                    bucket_name: bucket_name.clone(),
                    file_url,
                    crs: crs.clone(),
                    file_size: size as i64,
                    checksum: checksum.clone(),
                };

                if let Err(e) = db::insert_file_record(&pool_clone, &record).await {
                    eprintln!("Failed to insert metadata to Postgres: {}", e);
                }
            }

            drop(permit);
            result.map(|(size, _, _, _)| size)
        });

        handles.push(handle);
    }

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
                eprintln!("Upload failed: {}", e);
                failed += 1;
            }
            Err(e) => {
                eprintln!("Task panicked: {}", e);
                failed += 1;
            }
        }
    }

    let elapsed = start_time.elapsed();
    main_pb.finish_with_message("Migration complete!");
    bytes_pb.finish();

    println!("\nMigration Summary");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Successful: {}", successful);
    println!("Failed: {}", failed);
    println!("Total files: {}", successful + failed);
    println!("Data uploaded: {}", HumanBytes(total_uploaded));
    println!("Time elapsed: {:.2}s", elapsed.as_secs_f32());

    if successful > 0 && elapsed.as_secs_f32() > 0.0 {
        let speed = total_uploaded as f64 / elapsed.as_secs_f64();
        println!("Average speed: {:.2} MB/s", speed / (1024.0 * 1024.0));
    }

    // TODO: Process raw TIFFs into NDVI, NDWI and MNDWI index maps
    // This pipeline will read from the raw/ folder in MinIO, compute spectral indices,
    // and store the resulting index TIFFs in the processed/ folder.

    println!("\nMinIO Console: http://localhost:9001");
    println!("Login: minioadmin / minioadmin");

    Ok(())
}
