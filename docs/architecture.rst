.. _architecture:

Architecture Overview
=====================

This page describes the architecture of the Unified Data Governance Platform.
For the **Soda → Collibra integration sequence diagram** (Mermaid), see the main
``README.md`` in the repository root.

Three-Pillar Integration
------------------------

The platform connects data engineering, data quality, and data governance:

.. code-block:: text

   Data Engineering (dbt + Airflow)
            ↓
       Data Pipeline
            ↓
   Data Quality (Soda) ────→ Data Governance (Collibra)
            ↓                        ↓
       Quality Checks          Asset Mapping
            ↓                        ↓
       Quality Results ───────→ Governance Catalog

Complete Data Flow
------------------

Each layer follows **Build → Validate → Govern**. Quality checks **gate** metadata
synchronization: Collibra only syncs data that has passed quality validation.

.. code-block:: text

   RAW Layer (Snowflake)
       ↓
   Soda Quality Checks (RAW) [VALIDATION PHASE]
       ↓
   Collibra Metadata Sync (RAW Schema) [GOVERNANCE PHASE - GATED BY QUALITY]
       ↓
   dbt Transformations (STAGING) [BUILD PHASE]
       ↓
   Soda Quality Checks (STAGING) [VALIDATION PHASE]
       ↓
   Collibra Metadata Sync (STAGING Schema) [GOVERNANCE PHASE - GATED BY QUALITY]
       ↓
   dbt Models (MARTS) [BUILD PHASE]
       ↓
   Soda Quality Checks (MARTS) [VALIDATION PHASE]
       ↓
   Collibra Metadata Sync (MART Schema) [GOVERNANCE PHASE - GATED BY QUALITY]
       ↓
   Soda Quality Checks (QUALITY) + dbt Tests [VALIDATION PHASE]
       ↓
   Collibra Metadata Sync (QUALITY Schema) [GOVERNANCE PHASE - GATED BY QUALITY]
       ↓
   Cleanup Artifacts [CLEANUP PHASE]
       ↓
   Soda Cloud Dashboard + Collibra Integration

Orchestration Philosophy
------------------------

- **Quality gates metadata sync**: Metadata sync only happens after quality validation.
- **Collibra reflects commitments**: Only validated data enters the governance catalog.
- **Historical record**: Collibra becomes a record of accepted states, not a live mirror of all data.

Clean Architecture Layers
--------------------------

The application code (``src/``) follows a layered design:

.. code-block:: text

   ┌─────────────────────────────────────────┐
   │         Presentation Layer               │
   │  (Scripts, Airflow DAGs, CLI)             │
   └─────────────────────────────────────────┘
                       ↓
   ┌─────────────────────────────────────────┐
   │         Service Layer                   │
   │  (Business Logic Orchestration)          │
   │  - PipelineService                      │
   │  - QualityService                       │
   │  - MetadataService                      │
   └─────────────────────────────────────────┘
                       ↓
   ┌─────────────────────────────────────────┐
   │         Repository Layer                │
   │  (Data Access Abstraction)               │
   │  - SodaRepository                       │
   │  - CollibraRepository                   │
   └─────────────────────────────────────────┘
                       ↓
   ┌─────────────────────────────────────────┐
   │         Core Infrastructure             │
   │  (Config, Logging, Exceptions, Retry)   │
   └─────────────────────────────────────────┘

Design Patterns
---------------

Repository Pattern
  **Purpose**: Abstract data access logic.  
  **Implementation**: ``BaseRepository`` → ``SodaRepository``, ``CollibraRepository``.

Service Layer Pattern
  **Purpose**: Orchestrate business logic.  
  **Implementation**: ``PipelineService``, ``QualityService``, ``MetadataService``.

Factory Pattern
  **Purpose**: Centralized object creation with dependency injection.  
  **Implementation**: ``ClientFactory``, ``ServiceFactory``.

Singleton Pattern
  **Purpose**: Single configuration instance.  
  **Implementation**: ``get_config()`` function.

Component Overview
------------------

- **Core** (``src/core/``): Configuration (Pydantic), logging, exceptions, retry, health checks.
- **Repositories** (``src/repositories/``): HTTP/API access to Soda Cloud and Collibra.
- **Services** (``src/services/``): Pipeline orchestration, quality validation, metadata sync.
- **Factories** (``src/factories/``): Create repositories and services with injected config.

For detailed API documentation, see :doc:`api/index`.
