# Blob Sync

A Rust CLI tool for migrating local files to MinIO object storage. Recursively uploads directories with concurrent uploads, multipart support for large files, optional SHA-256 checksums, and real-time progress bars.

## Features

- **Concurrent uploads** with configurable parallelism (default: 5)
- **Multipart upload** for files over 50MB (10MB chunks)
- **SHA-256 checksums** (optional, enabled by default)
- **Progress tracking** with file count and byte throughput bars
- **S3-compatible** -- works with MinIO or any S3-compatible storage
- **Environment-based configuration** via `.env` file

## Prerequisites

- **Rust** (edition 2021) -- install via [rustup](https://rustup.rs/)
- **Docker** -- for running MinIO locally
- **Docker Compose** (optional) -- for easier MinIO setup

## MinIO Docker Setup

### Option 1: Docker Compose (Recommended)

Create a `docker-compose.yml` at the project root:

```yaml
services:
  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"   # S3 API
      - "9001:9001"   # Web Console
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio-data:/data
    command: server /data --console-address ":9001"
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  minio-data:
```

Start MinIO:

```bash
docker compose up -d
```

### Option 2: Docker Run

```bash
docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  -v minio-data:/data \
  minio/minio server /data --console-address ":9001"
```

### Verify MinIO is Running

```bash
docker ps
docker logs minio
```

Open the MinIO Console at **http://localhost:9001** and log in with:
- **Username:** `minioadmin`
- **Password:** `minioadmin`

### Create a Bucket

```bash
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/my-bucket
```

## Installation

```bash
git clone <repository-url>
cd Rust-Migration
cd blob-sync
cargo build --release
```

## Configuration

Create a `.env` file in the project root:

```env
MINIO_ENDPOINT=http://localhost:9000
MINIO_BUCKET=my-bucket
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
SOURCE_DIR=./data
CONCURRENT_UPLOADS=5
ENABLE_CHECKSUM=true
```

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `MINIO_ENDPOINT` | `http://localhost:9000` | MinIO server URL |
| `MINIO_BUCKET` | `sentinel-data-large` | Target bucket name |
| `MINIO_ACCESS_KEY` | `minioadmin` | S3 access key |
| `MINIO_SECRET_KEY` | `minioadmin` | S3 secret key |
| `SOURCE_DIR` | `./test-data-large` | Local directory to upload |
| `CONCURRENT_UPLOADS` | `5` | Max parallel uploads |
| `ENABLE_CHECKSUM` | `true` | Enable SHA-256 checksums |

## Usage

```bash
cargo run --release

# Override settings via environment variables
SOURCE_DIR=./my-data MINIO_BUCKET=other-bucket cargo run --release
```

## How It Works

1. Loads configuration from `.env`
2. Connects to MinIO via S3-compatible API
3. Recursively scans the source directory for files
4. Uploads files concurrently with a semaphore-limited thread pool
5. Files <= 50MB use single PUT; files > 50MB use multipart upload (10MB chunks)
6. Optionally computes SHA-256 checksums for each file
7. Displays real-time progress bars and a final summary

## Stopping MinIO

```bash
# Docker Compose
docker compose down

# Docker
docker stop minio && docker rm minio
```
