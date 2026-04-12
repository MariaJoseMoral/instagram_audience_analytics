PYTHON ?= python3

.PHONY: install extract transform pipeline pipeline-fast

install:
	$(PYTHON) -m pip install -r requirements.txt

extract:
	$(PYTHON) scripts/instagram_pipeline.py extract

transform:
	$(PYTHON) scripts/instagram_pipeline.py transform

pipeline:
	$(PYTHON) scripts/instagram_pipeline.py all

pipeline-fast:
	$(PYTHON) scripts/instagram_pipeline.py all --skip-media
