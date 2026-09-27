# AIO Data Ingestion

Daily crawler pipeline that pulls AI-related content from arXiv, YouTube, and Unsplash into a local data lake (Bronze → Silver → Gold).

## Sources

| Job key | Content | API |
|---------|---------|-----|
| `arxiv` | ML/CV papers (cs.LG, cs.CV) | arXiv Atom API (free, no key) |
| `yt` | AI tutorial videos | YouTube Data API v3 + yt-dlp |
| `unsplash` | Tech images | Unsplash REST API |

---

## Setup

```bash
pip install -r requirements.txt
```

Create API key files before running YouTube or Unsplash crawlers:

```
ingestion/youtube/.env
  YOUTUBE_API_KEY=<your YouTube Data API v3 key>

ingestion/unsplash/.env
  ACCESS_KEY=<your Unsplash access key>
```

Shell environment variables take precedence over `.env` files.

---

## Running

```bash
# all sources
python run.py --job all

# individual
python run.py --job arxiv
python run.py --job yt
python run.py --job unsplash

# multiple
python run.py --job arxiv --job unsplash
```

Each run appends a summary to `data-lake/gold/run_summary/YYYY-MM-DD.json`. Multiple runs on the same day append to `runs[]` rather than overwriting.

Logs are written to `logs/<source>_YYYY-MM-DD.log` (gitignored).

---

## Data lake layout

```
data-lake/
├── bronze/<source>/year=YYYY/month=MM/day=DD/   raw, immutable — never modify
├── silver/<source>/year=YYYY/month=MM/day=DD/   cleaned, typed Parquet
├── gold/run_summary/YYYY-MM-DD.json             daily execution summary
└── _state/<source>_ids.parquet                  dedup checkpoint (tracked in git)
```

Partitions follow Hive convention so DuckDB, Spark, and pandas can read them with `hive_partitioning=true`.

### Silver schemas

**arxiv** — one row per paper:

| Column | Type | Notes |
|--------|------|-------|
| `arxiv_id` | string | e.g. `2401.00001v1` |
| `title` | string | |
| `authors` | string | comma-separated |
| `abstract` | string | |
| `categories` | string | all arXiv category tags |
| `primary_category` | string | category used in the search query |
| `published` | datetime64[UTC] | |
| `updated` | datetime64[UTC] | |
| `pdf_url` | string | direct PDF link |
| `crawled_at` | string | ISO 8601 UTC |

**youtube** — one row per downloaded video:

| Column | Type | Notes |
|--------|------|-------|
| `video_id` | string | YouTube video ID |
| `title` | string | |
| `channel_id` | string | |
| `channel_title` | string | |
| `upload_date` | string | YYYYMMDD |
| `duration_s` | Int64 | seconds |
| `view_count` | Int64 | |
| `like_count` | Int64 | |
| `language` | string | |
| `bronze_video_path` | string | absolute path to .mp4 |
| `bronze_audio_path` | string | absolute path to .m4a |
| `bronze_info_path` | string | absolute path to .info.json |
| `crawled_at` | string | ISO 8601 UTC |

**unsplash** — one row per downloaded image:

| Column | Type | Notes |
|--------|------|-------|
| `photo_id` | string | Unsplash photo ID |
| `description` | string | |
| `alt_description` | string | |
| `width` | Int64 | pixels |
| `height` | Int64 | pixels |
| `color` | string | dominant hex color |
| `created_at` | datetime64[UTC] | |
| `photographer_name` | string | |
| `photographer_username` | string | |
| `unsplash_url` | string | photo page URL |
| `bronze_file_path` | string | absolute path to downloaded image |
| `crawled_at` | string | ISO 8601 UTC |

### Querying Silver with DuckDB

```python
import duckdb

# arXiv papers from a specific month
duckdb.sql("""
    SELECT arxiv_id, title, authors, published
    FROM read_parquet('data-lake/silver/arxiv/**/*.parquet', hive_partitioning=true)
    WHERE year = '2024' AND month = '06'
    ORDER BY published DESC
""")

# YouTube videos by duration
duckdb.sql("""
    SELECT video_id, title, duration_s, view_count
    FROM read_parquet('data-lake/silver/youtube/**/*.parquet', hive_partitioning=true)
    WHERE duration_s BETWEEN 120 AND 720
""")
```

---

## Subtitle conversion

YouTube subtitles are downloaded as `.vtt` or `.srt` into `bronze/youtube/.../subs/`. Convert to plain text:

```bash
# process all subtitles under bronze/youtube/
python ingestion/youtube/script_to_text.py

# specific directory
python ingestion/youtube/script_to_text.py --input data-lake/bronze/youtube/year=2024/month=06/day=15/subs

# single file
python ingestion/youtube/script_to_text.py --input path/to/file.vtt --output path/to/out.txt
```

Output `.txt` files land in a `text/` sibling directory next to each `subs/` directory.

---

## Scheduling

**Linux (cron):**

```bash
# every day at 08:00 — edit with: crontab -e
0 8 * * * cd /path/to/aio-data-ingestion && python run.py --job all >> logs/cron.log 2>&1
```

**Windows (Task Scheduler):**

```powershell
$python  = "C:\path\to\python.exe"
$workDir = "D:\path\to\aio-data-ingestion"

$action   = New-ScheduledTaskAction -Execute $python -Argument "run.py --job all" -WorkingDirectory $workDir
$trigger  = New-ScheduledTaskTrigger -Daily -At 8:00AM
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopOnIdleEnd
Register-ScheduledTask -TaskName "AIO Ingestion Daily" -Action $action -Trigger $trigger -Settings $settings
```

---

## More documentation

- [Architecture](docs/architecture.md) — data lake zones, state management, atomic writes
- [Crawlers](docs/crawlers.md) — per-source configuration reference
