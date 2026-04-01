# DataPipelineEnv

DataPipelineEnv is a production-ready OpenEnv hackathon backend for evaluating
agentic debugging and remediation of broken enterprise data pipelines.

## Tasks

1. Data Quality Audit (easy)
2. Schema Drift Remediation (medium)
3. Full Data Incident Response (hard)

## Quick Start

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
uvicorn env.server:app --port 8000 --workers 1
```

## Run Agent

```bash
python inference.py
```

## Verify

```bash
python -m pytest tests/test_env.py -v --tb=short
```