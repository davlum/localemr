#!/bin/bash
set -o errexit -o nounset -o pipefail

# Install dev requirements
pip install -r requirements-dev.txt -q

# run linter
flake8 localemr test
pylint localemr test

# run tests
pytest -vv
