Unified Data Governance Platform Documentation
==============================================

Welcome to the Unified Data Governance Platform documentation.

This platform integrates Soda Cloud and Collibra for comprehensive
data governance and quality monitoring.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   architecture
   examples
   api/index

Architecture Overview
---------------------

The platform follows a clean architecture pattern with:

* **Core Layer**: Configuration, logging, exceptions, retry logic
* **Repository Layer**: Data access abstraction for external APIs
* **Service Layer**: Business logic orchestration
* **Factory Layer**: Dependency injection and object creation

Quick Start
-----------

.. code-block:: python

   from src.core.config import get_config
   from src.services.pipeline_service import PipelineService
   
   config = get_config()
   pipeline = PipelineService(config=config)
   result = pipeline.run_complete_pipeline(layers=["raw", "staging"])

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
