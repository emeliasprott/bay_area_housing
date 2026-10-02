#!/bin/zsh
set -euo pipefail
cd -- "${0:A:h}"
for script in spatial acs market hmda; do
    "${PYTHON:-python3}" "$script.py"
done
