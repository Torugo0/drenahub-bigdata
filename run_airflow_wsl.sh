#!/usr/bin/env bash
# Executar dentro do Ubuntu WSL2, a partir de qualquer diretório.
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HOME/.venvs/drenahub-airflow/bin/activate"
export AIRFLOW_HOME="$HOME/.airflow-drenahub"
export AIRFLOW__CORE__DAGS_FOLDER="$repo_dir/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

exec airflow standalone
