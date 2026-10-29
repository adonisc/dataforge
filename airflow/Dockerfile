# Pin the exact version so rebuilds are reproducible.
FROM apache/airflow:3.3.0-python3.12

USER root
# Keep system packages to the minimum.
RUN apt-get update \
 && apt-get install --no-install-recommends -y build-essential curl git \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# Always install with Airflow's constraint file.
ARG AIRFLOW_VERSION=3.3.0
ARG PYTHON_VERSION=3.12
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt --constraint "${CONSTRAINT_URL}"

# Install dataforge as a package.
COPY --chown=airflow:root pyproject.toml /opt/dataforge/pyproject.toml
COPY --chown=airflow:root src /opt/dataforge/src
COPY --chown=airflow:root README.md /opt/dataforge/README.md
# Includes dev extras because this is the only image. A prod image would skip them.
RUN pip install --no-cache-dir -e "/opt/dataforge[duckdb,aws,dev]"

# Install dbt packages at build time.
ENV DBT_PACKAGES_PATH=/opt/dbt-packages
COPY --chown=airflow:root transform/dbt /opt/dbt-build-context
RUN cd /opt/dbt-build-context && dbt deps --no-version-check && \
    test -d /opt/dbt-packages && rm -rf /opt/dbt-build-context

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DBT_PROFILES_DIR=/opt/airflow/transform/dbt
