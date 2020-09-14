#!/bin/bash
set -o errexit -o nounset -o pipefail

# run linter
pipenv run flake8 localemr test
pipenv run pylint localemr test

# run tests
pipenv run pytest -vv
