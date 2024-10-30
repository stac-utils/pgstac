#!/bin/bash
uv venv
source .venv/bin/activate
uv sync --no-install-project --extra test --extra dev --extra 
