# SAM build target — installs Lambda-safe deps + copies source
# Invoked automatically by: sam build

.PHONY: build-AskMyBankFunction

build-AskMyBankFunction:
	/opt/homebrew/bin/python3 -m pip install \
		--platform manylinux2014_x86_64 \
		--implementation cp \
		--python-version 3.11 \
		--only-binary=:all: \
		-r requirements-lambda.txt \
		-t $(ARTIFACTS_DIR)
	cp lambda_handler.py $(ARTIFACTS_DIR)/
	cp -r rag $(ARTIFACTS_DIR)/rag
	mkdir -p $(ARTIFACTS_DIR)/workflows/temporal
	cp workflows/temporal/config.py $(ARTIFACTS_DIR)/workflows/temporal/config.py
	touch $(ARTIFACTS_DIR)/workflows/__init__.py $(ARTIFACTS_DIR)/workflows/temporal/__init__.py
