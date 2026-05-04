# SAM build target — installs Lambda-safe deps + copies source
# Invoked automatically by: sam build

.PHONY: build-AskMyBankFunction

build-AskMyBankFunction:
	pip install \
		--platform manylinux2014_x86_64 \
		--implementation cp \
		--python-version 3.11 \
		--only-binary=:all: \
		-r requirements-lambda.txt \
		-t $(ARTIFACTS_DIR)
	cp lambda_handler.py $(ARTIFACTS_DIR)/
	cp -r rag $(ARTIFACTS_DIR)/rag
