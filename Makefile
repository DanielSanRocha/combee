help:   ## Show this help.
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

clean:  ## Clean the project
	rm -rf tmp/*.csv
	rm  -rf tmp/*.parquet
	rm -rf target

test:   ## Run integration tests
	rm  -rf tmp/*.csv
	rm  -rf tmp/*.parquet
	cargo test

upload: ## Upload version to crates.io
	cargo publish

doc: ## Generate and open local documentation
	cargo doc
	xdg-open target/doc/combee/index.html
