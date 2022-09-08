set dotenv-load := true

# initialize repository
init:
    @echo 'install dev dependencies.'
    poetry install
    poetry run pip install --upgrade pip
    just develop

# build development version of packages
develop:
    poetry run maturin develop -m object-store/Cargo.toml

# build native packages
build:
    @echo 'build and install object-store.'
    poetry run pip install -e ./object-store/

# run object-store tests
test-py:
    pytest object-store/

# run all tests
test: test-py

# serve the documentation
serve:
    mkdocs serve
