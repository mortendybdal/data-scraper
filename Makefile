.PHONY: login upload reformat merge

reformat:
	python scripts/reformat_sft.py output/combined_cpt.jsonl output/combined_sft.jsonl

merge:
	python main.py --merge

login:
	hf auth login

upload: output/combined_cpt.jsonl
	hf upload mortendybdal/gp-healthcare-dk output/combined_cpt.jsonl --repo-type=dataset
