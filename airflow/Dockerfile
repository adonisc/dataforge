# Slim image: only the providers listed in requirements.txt, so fewer packages to
# patch. Pin the exact version so rebuilds are reproducible.
FROM apache/airflow:slim-3.3.2-python3.12

USER root
# Keep system packages to the minimum, with current security patches.
RUN apt-get update \
 && apt-get upgrade -y \
 && apt-get install --no-install-recommends -y build-essential curl git \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
# Folders the airflow user writes to at build time.
RUN mkdir -p /opt/dbt /opt/dataforge && chown airflow:root /opt/dbt /opt/dataforge

USER airflow

# Always install with Airflow's constraint file.
ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt --constraint "${CONSTRAINT_URL}"

# dbt gets its own virtualenv: its pins (e.g. pathspec) conflict with Airflow's
# constraints, and the two upgrade on different schedules.
COPY airflow/requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/dbt/venv \
 && /opt/dbt/venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt
# Appended, so `python` and `pip` still resolve to Airflow's environment.
ENV PATH="${PATH}:/opt/dbt/venv/bin"

# Install dataforge as a package, within Airflow's constraints.
COPY --chown=airflow:root pyproject.toml /opt/dataforge/pyproject.toml
COPY --chown=airflow:root src /opt/dataforge/src
COPY --chown=airflow:root README.md /opt/dataforge/README.md
# Includes dev extras because this is the only image. A prod image would skip them.
RUN pip install --no-cache-dir -e "/opt/dataforge[duckdb,aws,dev]" --constraint "${CONSTRAINT_URL}"

# Security patches newer than Airflow's constraints. Safe to override: certifi is CA
# data and uv is an installer, neither is imported by Airflow at runtime. Drop these
# once the constraints catch up.
COPY airflow/requirements-security.txt /tmp/requirements-security.txt
RUN pip install --no-cache-dir --upgrade -r /tmp/requirements-security.txt \
 && /opt/dbt/venv/bin/pip install --no-cache-dir --upgrade -r /tmp/requirements-security.txt

# Install dbt packages at build time.
ENV DBT_PACKAGES_PATH=/opt/dbt/packages
COPY --chown=airflow:root transform/dbt /tmp/dbt-build-context
RUN cd /tmp/dbt-build-context && dbt deps --no-version-check \
 && test -d /opt/dbt/packages/dbt_utils && rm -rf /tmp/dbt-build-context

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DBT_PROFILES_DIR=/opt/airflow/transform/dbt
