.. _examples:

Usage Examples
==============

The project includes runnable examples in ``examples/basic_usage.py``. This page
shows the main patterns; run the script from the project root with the virtual
environment activated.

Prerequisites
-------------

- Project dependencies installed (e.g. ``just deps``).
- ``.env`` configured with Snowflake, Soda Cloud, and Collibra credentials.

Basic Pipeline (Single Layer)
------------------------------

Run quality checks for the raw layer and sync metadata with a quality gate:

.. code-block:: python

   from src.core.config import get_config
   from src.services.pipeline_service import PipelineService

   config = get_config()
   pipeline = PipelineService(config=config)

   result = pipeline.run_quality_checks("raw")
   sync_result = pipeline.sync_metadata_with_quality_gate("raw", strict=True)

Fetch Data from Soda Cloud API
------------------------------

Use the repository to list datasets and checks:

.. code-block:: python

   from src.core.config import get_config
   from src.repositories.soda_repository import SodaRepository

   config = get_config()
   repo = SodaRepository(config=config)

   with repo:
       datasets = repo.get_all_datasets()
       checks = repo.get_all_checks()

Health Check
------------

Check platform health (config, Soda Cloud, Collibra):

.. code-block:: python

   from src.core.health import HealthChecker

   checker = HealthChecker()
   summary = checker.get_health_summary()
   result = checker.check_all()

Complete Pipeline (All Layers)
-------------------------------

Run the full pipeline for raw, staging, and mart layers:

.. code-block:: python

   from src.core.config import get_config
   from src.services.pipeline_service import PipelineService

   config = get_config()
   pipeline = PipelineService(config=config)

   result = pipeline.run_complete_pipeline(
       layers=["raw", "staging", "mart"],
       strict=False,
   )

Running the Examples
--------------------

From the project root:

.. code-block:: bash

   # Activate venv (e.g. source .venv/bin/activate or use uv run)
   python examples/basic_usage.py

Uncomment the desired example calls in ``if __name__ == "__main__":`` in
``examples/basic_usage.py`` to execute them.
