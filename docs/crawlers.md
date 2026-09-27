# Crawlers

Each crawler lives in `ingestion/<source>/` and exposes a single `main()` function that returns:

```python
{"source": str, "status": "ok"|"partial"|"error", "count": int, "errors": list[str]}
```

Configuration is loaded from `config.yaml` in the same directory.

---

## arXiv

**File:** `ingestion/arxiv/crawler.py`  
**Config:** `ingestion/arxiv/config.yaml`  
**API key:** none required

### Configuration

```yaml
categories:
  - cs.LG   # any arXiv category taxonomy code
  - cs.CV

max_results_per_category: 50   # per API call
days_back: 1                   # search window: now - N days → now

api:
  base_url: "https://export.arxiv.org/api/query"
  request_delay: 5     # seconds to wait before each request (rate limit courtesy)
  timeout: 60          # HTTP timeout in seconds
  max_retries: 6
  retry_backoff: 15    # backoff multiplier in seconds
```

### Behavior

1. For each `category`, queries the arXiv Atom API with a `submittedDate` range filter.
2. Deduplicates across categories — a paper in both `cs.LG` and `cs.CV` is written once.
3. Writes raw entries to Bronze as `entries.json`.
4. Writes cleaned Parquet to Silver with `published`/`updated` parsed as `datetime64[UTC]`.

### Adding categories

Find valid taxonomy codes at <https://arxiv.org/category_taxonomy>. Add them to `categories:` in `config.yaml`.

---

## YouTube

**File:** `ingestion/youtube/crawler.py`  
**Config:** `ingestion/youtube/config.yaml`  
**API key:** `ingestion/youtube/.env` → `YOUTUBE_API_KEY=...`

### Configuration

```yaml
search:
  query: "AI Research Papers"       # YouTube search query
  relevance_language: en            # hint to YouTube ranking
  require_english: true             # filter: skip non-English titles/metadata
  days_back: 1                      # publishedAfter = now - N days
  max_results: 3                    # target number of videos to download
  min_duration_seconds: 120         # skip videos shorter than this
  max_duration_seconds: 720         # skip videos longer than this
  exclude_shorts: true              # skip /shorts/ URLs
  search_timeout_seconds: 3600      # wall-clock deadline for the search phase

download:
  video_format: "bestvideo[ext=mp4]/bestvideo/best[ext=mp4]/best"
  audio_format: "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio/best"
  write_info_json: true
  subtitles:
    enabled: true
    languages: ["en"]
    write_auto_subs: true           # fall back to auto-generated captions
    formats: "vtt/srt"             # preferred subtitle format

api:
  request_delay: 1.0               # seconds between YouTube API calls
  max_retries: 6
  retry_backoff: 15.0
```

### Behavior

**Search phase** (YouTube Data API):
1. Calls `search.list` with `type=video`, `order=date`, paging until `max_results` are collected or the timeout is hit.
2. For each page of results, calls `videos.list` to get `snippet` + `contentDetails` (duration, language).
3. Filters: English check (`defaultAudioLanguage`, `defaultLanguage`, or Latin-character heuristic), duration range, Shorts exclusion, and already-ingested IDs.

**Download phase** (yt-dlp) — three independent passes per video:
1. **Meta + subs** — `skip_download=True`, writes `.info.json` and `.vtt/.srt` subtitle files.
2. **Video** — downloads best mp4 to `bronze/.../video/<id>.mp4`.
3. **Audio** — downloads best m4a to `bronze/.../audio/<id>.m4a`.

Each pass is wrapped in its own `try/except`, so a failed video download does not prevent audio or subtitle extraction.

### YouTube API quota

The YouTube Data API has a daily quota of 10,000 units. Costs per call:
- `search.list`: 100 units
- `videos.list`: 1 unit per call (batched, so ~1 unit per page)

With `max_results: 3` and typical paging, a single run uses ~100–200 units.

---

## Unsplash

**File:** `ingestion/unsplash/crawler.py`  
**Config:** `ingestion/unsplash/config.yaml`  
**API key:** `ingestion/unsplash/.env` → `ACCESS_KEY=...`

### Configuration

```yaml
search:
  query: technology               # Unsplash search query
  order_by: latest                # latest | relevant
  today_only: true                # only include photos created today
  max_results: 20                 # maximum photos to download
  per_page: 30                    # results per API page (max 30)
  orientation: null               # landscape | portrait | squarish | null (any)
  color: null                     # black_and_white | blue | green | ... | null

download:
  image_quality: regular          # raw | full | regular | small | thumb

api:
  base_url: "https://api.unsplash.com"
  request_delay: 1.0              # seconds between API calls
  max_retries: 6
  retry_backoff: 15.0
  download_timeout: 60            # HTTP timeout for image downloads
```

### Behavior

1. Calls `GET /search/photos` with pagination until `max_results` photos are collected.
2. If `today_only: true`, skips photos whose `created_at` date is not today (local time) and stops paging once all results on a page are older than today.
3. For each photo, triggers `download_location` (required by [Unsplash API Terms of Service](https://unsplash.com/api-terms)) before downloading the image.
4. Saves raw JSON to Bronze as `search_results.json`, images to Bronze under `images/<id>.<ext>`.
5. Writes Silver Parquet with `created_at` as `datetime64[UTC]` and dimensions as `Int64`.

### Unsplash ToS compliance

The `download_location` endpoint must be called before serving or downloading each image. The crawler calls it automatically via `_api_get(_append_client_id(dl_loc, access_key), ...)`. Do not remove this step.

### Rate limits

Unsplash Demo apps: 50 requests/hour. Production apps: 5,000 requests/hour. Each run uses `ceil(max_results / per_page) + max_results` requests (search pages + image downloads).

---

## script_to_text

**File:** `ingestion/youtube/script_to_text.py`

Converts `.vtt` / `.srt` subtitle files to clean plain text. Strips timestamps, HTML tags, duplicate consecutive lines, and VTT headers.

```bash
# process all subtitles under bronze/youtube/ (default)
python ingestion/youtube/script_to_text.py

# specific subs directory
python ingestion/youtube/script_to_text.py \
  --input data-lake/bronze/youtube/year=2024/month=06/day=15/subs

# single file with explicit output
python ingestion/youtube/script_to_text.py \
  --input path/to/file.vtt \
  --output path/to/out.txt
```

Default output path: `<day_dir>/text/<video_id>.txt` (sibling to `subs/`).
