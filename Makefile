.PHONY: login upload reformat merge clean pipeline

# --- Scrape a specific site (usage: make scrape SITE=vaccination-dk) ---
scrape:
	@if [ -z "$(SITE)" ]; then echo "Usage: make scrape SITE=<site-name>"; exit 1; fi
	python main.py --sites $(SITE)

# --- Clean all per-site JSONL files (dedup, boilerplate removal) ---
clean:
	python scripts/clean.py output/*.jsonl --keep-rejected -v

# --- Merge per-site files into combined_cpt.jsonl ---
merge:
	python main.py --merge

# --- Reformat combined file to SFT format ---
reformat:
	python scripts/reformat_sft.py output/combined_cpt.jsonl output/combined_sft.jsonl

# --- Full pipeline: clean → merge → upload ---
pipeline: clean merge upload

# --- HuggingFace ---
login:
	hf auth login

upload: output/combined_cpt.jsonl
	hf upload mortendybdal/gp-healthcare-dk output/combined_cpt.jsonl --repo-type=dataset
