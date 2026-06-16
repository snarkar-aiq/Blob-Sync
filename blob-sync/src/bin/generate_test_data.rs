use anyhow::Result;
use rand::Rng;
use std::fs;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

async fn create_large_file(path: &str, size_mb: u64) -> Result<()> {
    println!("   Creating {} ({} MB)", path, size_mb);

    let mut file = File::create(path).await?;
    let chunk_size = 1024 * 1024; // 1MB chunks
    let chunks = size_mb;
    let mut rng = rand::thread_rng();
    let mut buffer = vec![0u8; chunk_size];

    for i in 0..chunks {
        if i % 100 == 0 && i > 0 {
            print!(".");
            std::io::Write::flush(&mut std::io::stdout())?;
        }
        rng.fill(&mut buffer[..]);
        file.write_all(&buffer).await?;
    }
    println!(" ✓");

    Ok(())
}

async fn create_scene(scene_name: &str, base_path: &str) -> Result<u64> {
    let scene_dir = format!("{}/{}", base_path, scene_name);
    fs::create_dir_all(&scene_dir)?;

    let mut total_size = 0;

    // Define bands with realistic sizes for Sentinel-2
    let bands = vec![
        // 10m resolution bands (largest)
        ("B02", 250),
        ("B03", 250),
        ("B04", 250),
        ("B08", 200),
        // 20m resolution bands (medium)
        ("B05", 80),
        ("B06", 80),
        ("B07", 80),
        ("B8A", 80),
        ("B11", 80),
        ("B12", 80),
        // 60m resolution bands (smallest)
        ("B01", 15),
        ("B09", 15),
        ("B10", 15),
    ];

    for (band_name, size_mb) in bands {
        let file_path = format!("{}/{}.jp2", scene_dir, band_name);
        create_large_file(&file_path, size_mb).await?;
        total_size += size_mb;
    }

    // Create metadata files
    let cloud_cover = rand::thread_rng().gen_range(0.0..30.0);
    let metadata = format!(
        r#"{{
    "product": "{}",
    "date": "2025-01-01",
    "tile": "T32TPP",
    "cloud_cover": {:.1},
    "scene_count": 1
}}"#,
        scene_name, cloud_cover
    );

    fs::write(format!("{}/metadata.json", scene_dir), metadata)?;

    // Create auxiliary files
    create_large_file(&format!("{}/aux_data.bin", scene_dir), 50).await?;
    total_size += 50;

    // Create quality band
    create_large_file(&format!("{}/quality_band.bin", scene_dir), 30).await?;
    total_size += 30;

    Ok(total_size)
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 Generating large Sentinel-2 test dataset");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let base_path = "./test-data-large";
    fs::create_dir_all(base_path)?;

    // Generate multiple scenes to reach ~10GB
    let scene_names = vec![
        "S2A_20250101_T32TPP",
        "S2A_20250102_T32TPP",
        "S2B_20250101_T32TPP",
        "S2B_20250102_T32TPP",
        "S2A_20250103_T32TPP",
        "S2B_20250103_T32TPP",
        "S2A_20250104_T32TPP",
        "S2B_20250104_T32TPP",
        "S2A_20250105_T32TPP",
        "S2B_20250105_T32TPP",
    ];

    let mut total_size_gb = 0.0;
    let mut total_files = 0;

    for (idx, scene) in scene_names.iter().enumerate() {
        println!(
            "\n📁 Creating scene {}/{}: {}",
            idx + 1,
            scene_names.len(),
            scene
        );
        let size_mb = create_scene(scene, base_path).await?;
        total_size_gb += size_mb as f64 / 1024.0;
        total_files += 17; // 13 bands + metadata + aux + quality
        println!(
            "   ✅ Created {} MB ({:.2} GB total so far, {} files)",
            size_mb, total_size_gb, total_files
        );
    }

    // Create a giant single file for extreme testing
    println!("\n📦 Creating giant test file (3GB) for multipart testing...");
    let giant_path = format!("{}/giant_test.bin", base_path);
    create_large_file(&giant_path, 3072).await?; // 3GB
    total_size_gb += 3.0;
    total_files += 1;

    println!("\n✅ Completed!");
    println!("📊 Total test data: {:.2} GB", total_size_gb);
    println!("📂 Location: {}", base_path);
    println!("📁 Number of scenes: {}", scene_names.len());
    println!("📄 Total files created: {}", total_files);
    println!("\n🎯 Ready for migration testing!");

    Ok(())
}
