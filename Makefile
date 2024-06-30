all:
	cargo build --all-features

.PHONY: test
test:
	cargo test --all-features

.PHONY: lint
lint:
	cargo clippy --all-features