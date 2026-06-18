CREATE TABLE IF NOT EXISTS uploaded_files (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_name TEXT NOT NULL,
    renamed_name TEXT NOT NULL,
    bucket_name TEXT NOT NULL DEFAULT 'sentinel-2',
    file_url TEXT NOT NULL,
    crs TEXT,
    file_size BIGINT,
    checksum TEXT,
    uploaded_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_uploaded_files_original_name ON uploaded_files(original_name);
CREATE INDEX IF NOT EXISTS idx_uploaded_files_renamed_name ON uploaded_files(renamed_name);
CREATE INDEX IF NOT EXISTS idx_uploaded_files_uploaded_at ON uploaded_files(uploaded_at);
