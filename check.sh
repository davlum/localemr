#!/bin/bash
set -o errexit -o nounset -o pipefail

# run linter
flake8 localemr test
pylint localemr test

# run tests
pytest -vv
