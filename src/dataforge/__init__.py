"""dataforge: a contract-driven data pipeline.

Each dataset is described by one contract YAML. Ingestion, schema, quality rules,
masking, layout, DAGs and lineage are all derived from it, so adding a dataset means
adding a YAML file (and maybe a dbt model), not writing new Python.
"""

from dataforge.contracts.models import DataContract
from dataforge.quality.engine import ValidationEngine, ValidationReport

__all__ = ["DataContract", "ValidationEngine", "ValidationReport"]
__version__ = "0.1.0"
