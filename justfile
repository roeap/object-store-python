set dotenv-load := true

_default:
    just --list

# initialize repository
init:
    poetry install --no-root
    poetry run pip install --upgrade pip
    poetry run pre-commit install
    just develop

# build development version of packages
develop:
    poetry run maturin develop -m object-store/Cargo.toml --extras=pyarrow

build:
    poetry run maturin build -m object-store/Cargo.toml --release

# run automatic code formatters
fix:
    poetry run black .
    poetry run ruff --fix .

# run object-store python tests
test-py:
    pytest object-store/ --benchmark-autosave --cov

# run object-store rust tests
test-rs:
    cargo test

# run all tests
test: test-rs test-py

# serve the documentation
serve:
    mkdocs serve
