use anyhow::Result;
use sqlx::PgPool;

pub struct FileRecord {
    pub original_name: String,
    pub renamed_name: String,
    pub bucket_name: String,
    pub file_url: String,
    pub crs: Option<String>,
    pub file_size: i64,
    pub checksum: Option<String>,
}

pub async fn insert_file_record(pool: &PgPool, record: &FileRecord) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO uploaded_files 
           (original_name, renamed_name, bucket_name, file_url, crs, file_size, checksum)
           VALUES ($1, $2, $3, $4, $5, $6, $7)"#,
    )
    .bind(&record.original_name)
    .bind(&record.renamed_name)
    .bind(&record.bucket_name)
    .bind(&record.file_url)
    .bind(&record.crs)
    .bind(record.file_size)
    .bind(&record.checksum)
    .execute(pool)
    .await?;
    Ok(())
}
