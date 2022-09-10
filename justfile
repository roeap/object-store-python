set dotenv-load := true

# initialize repository
init:
    poetry install --no-root
    poetry run pip install --upgrade pip
    pre-commit install
    just develop

# build development version of packages
develop:
    poetry run maturin develop -m object-store/Cargo.toml --extras=pyarrow

# run object-store python tests
test-py:
    pytest object-store/

# run object-store rust tests
test-rs:
    cargo test

# run all tests
test: test-rs test-py

# serve the documentation
serve:
    mkdocs serve
