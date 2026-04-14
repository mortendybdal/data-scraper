# DataScraper

Scrape Danish healthcare websites and produce cleaned JSONL datasets for LLM training (SFT format).

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Scraping (terminal commands)

```bash
# List all configured sites
python main.py --list

# Scrape a specific site
python main.py --sites vaccination-dk

# Scrape multiple sites
python main.py --sites vaccination-dk sportnetdoc-dk

# Scrape all configured sites
python main.py

# Verbose logging
python main.py --sites vaccination-dk -v

# Resume-aware (default) — skips already-crawled URLs
python main.py --sites vaccination-dk

# Recrawl from scratch (ignores saved state)
python main.py --sites vaccination-dk --recrawl

# Continuous mode — no max_pages limit, crawl until done or Ctrl+C
python main.py --sites vaccination-dk --continuous

# Merge all per-site JSONL files into combined_cpt.jsonl
python main.py --merge
```

Press **Ctrl+C** at any time to stop — data is saved after each batch.

## Makefile commands

| Command                   | Description                                                                                                                               |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `make scrape SITE=<name>` | Scrape a specific site (e.g. `make scrape SITE=vaccination-dk`)                                                                           |
| `make clean`              | Post-process all per-site JSONL files (dedup, boilerplate removal, cookie stripping). Writes `.rejected.jsonl` sidecar files for auditing |
| `make merge`              | Merge all per-site JSONL files into `output/combined_cpt.jsonl`                                                                           |
| `make reformat`           | Reformat `combined_cpt.jsonl` into SFT format (`combined_sft.jsonl`)                                                                      |
| `make pipeline`           | Run full pipeline: clean → merge → upload                                                                                                 |
| `make login`              | Authenticate with HuggingFace                                                                                                             |
| `make upload`             | Upload `combined_cpt.jsonl` to HuggingFace dataset                                                                                        |

## Typical workflow

```bash
# 1. Scrape sites
python main.py --sites vaccination-dk sportnetdoc-dk

# 2. Clean, merge, and upload
make pipeline
```

## Adding a new site

Add a block to `config/sites.yaml`:

```yaml
- name: "my-site"
  base_url: "https://example.com"
  start_urls:
    - "https://example.com/section/"
  follow_links: true
  link_selector: "a[href]"
  max_pages: 10000
  delay: 1.5
  allowed_paths:
    - "/section/"
```

## Output format

Each record in the JSONL files:

```json
{
  "instruction": "",
  "input": "",
  "output": "<extracted text>",
  "source": "site-name",
  "url": "https://...",
  "chunk": "1/3"
}
```
