# SGLang & vLLM Contributor Scraping Pipeline

This repo contains a small, reproducible data pipeline to scrape contributors and contribution stats
for two GitHub repositories: **SGLang** and **vLLM**.

It follows the structure:

```
.
├── data/
│   ├── raw/
│   └── clean/
├── pipeline/
│   ├── run.py
│   └── src/
│       ├── github_scrape.py
│       ├── utils.py
│       └── dummy_source.py
└── scratch/
```

## Key ideas
- `pipeline/run.py` orchestrates the pipeline.
- Each run creates a **timestamped folder** under `data/clean/` and writes all outputs there.
- A **log file** for the run is saved in the same timestamped folder.
- Source scripts live in `pipeline/src/` and are imported by `run.py`.
- A `dummy_source.py` step demonstrates the plumbing even without network access.

## Setup

1. (Optional but recommended) Create a virtual environment.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. (Recommended) Set a GitHub token for higher rate limits:
   - macOS/Linux:
     ```bash
     export GITHUB_TOKEN="YOUR_TOKEN"
     ```
   - Windows (PowerShell):
     ```powershell
     setx GITHUB_TOKEN "YOUR_TOKEN"
     ```

## Run

From the project root:

```bash
python pipeline/run.py
```
## Post-processing: PR user collection and CSV outputs

After a run that populates `by_topics/prs/<repo>/pull_requests.json`, you can fetch the full GitHub
user profiles for PR authors (and write per-repo CSVs) using the built-in helper `scrape_users_from_prs`.

From the project root (recommended: with `GITHUB_TOKEN` exported for higher rate limits):

```bash
python - <<'PY'
from pathlib import Path
from pipeline.src.topic_scrape import scrape_users_from_prs

latest = sorted(Path('data/clean').iterdir(), reverse=True)[0]
by_topics = latest / 'by_topics'
prs_parent = by_topics / 'prs'

print('Running users collection for', prs_parent)
scrape_users_from_prs(str(prs_parent), str(by_topics), logger=None)
print('Done — per-repo users JSON and CSVs are under', prs_parent)
print('Index written to', by_topics / 'pr_users_index.json')
PY
```

Outputs created/written:

- Per-repo: `data/clean/<TIMESTAMP>/by_topics/prs/<repo>/users.json` and `contributors.csv`
- Aggregate: `data/clean/<TIMESTAMP>/by_topics/repos_by_topic.csv`
- Index: `data/clean/<TIMESTAMP>/by_topics/pr_users_index.json` (maps repo -> users CSV/JSON)

Notes:
- Make sure `GITHUB_TOKEN` is set in your shell to avoid low unauthenticated rate limits.
- The code uses a rate-limit-aware request helper and will sleep/retry when limits are reached; large runs can still take long depending on GitHub quotas.


Outputs will appear under `data/clean/<TIMESTAMP>/`, e.g.,

```
data/clean/2025-11-06_11-00-00/
├── pipeline_run.log
├── MANIFEST.json
├── sglang/
│   ├── contributors.csv
│   ├── contributor_stats.json
│   └── users.csv
├── vllm/
│   ├── contributors.csv
│   ├── contributor_stats.json
│   └── users.csv
└── dummy/
    └── dummy_output.csv
```

> Note: The actual scraping calls the GitHub REST API. You need internet access when
> running locally. In this sandbox, the files are provided but network calls won't work.
