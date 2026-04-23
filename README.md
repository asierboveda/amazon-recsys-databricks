# amazon-recsys-databricks

Base de proyecto para un sistema de recomendación sobre Amazon con stack Python + Databricks.

## Estructura

- `src/amazon_recsys/`: código fuente
- `notebooks/`: notebooks y experimentos
- `tests/`: pruebas

## Setup rápido (Windows PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements-dev.txt
```

## Nota Databricks

Este repo está preparado para trabajar en local y migrar notebooks/jobs a Databricks después.

