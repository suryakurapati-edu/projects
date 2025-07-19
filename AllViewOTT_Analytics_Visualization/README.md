**AllView - OTT Platform Data Analysis**

**Introduction**

AllView is a new OTT platform designed to enhance the streaming experience. This project focuses on using data to drive the platform's success by analyzing competitors like Netflix, Amazon Prime, Hulu, and Disney+.

**Functional Overview**

The goal is to optimize content, guide global expansion, and target audience engagement through data-driven insights. Key components include a scalable ETL pipeline using the LIFE Framework and a real-time analytics dashboard to track KPIs.

**Tech Overview**

The technical architecture involves:

1.  Ingesting structured and semi-structured data.

2.  Using an ETL pipeline with the LIFE Framework (Load Integrate Filter Export).
   
3.  Three-layer pipeline:
   
    * Stage Layer: Loading data into PostgreSQL and MongoDB.
        
    * Processed Layer: Cleaning and transforming data into facts and dimensions in PostgreSQL.
        
    * Consumption Layer: Storing refined data for analytics in PostgreSQL.
        
4.  Feeding processed data into an analytics platform.

**Data Model**

The data model integrates data from Netflix, Disney Plus, Hulu, and Amazon Prime into a central fact table for AllView analysis (processed.fact\_ott).

**LIFE Framework**

LIFE is a custom Python framework for processing structured and unstructured data into SQL and NoSQL databases.

**Pipeline Stages**

* **Stage Layer:** Loads and transforms data, performs validation, deduplication, null checks, and adds surrogate keys before loading into PostgreSQL.
   
* **Processed Layer:** Implements business logic and history tracking (SCD type 2).