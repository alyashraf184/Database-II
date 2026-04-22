# Java DBMS Simulator

A lightweight database management system simulator built in Java, implementing core DBMS functionality using a page-based storage architecture.

## Overview

This project simulates how a real DBMS stores and retrieves data on disk. Rather than keeping everything in memory, data is persisted across sessions using Java serialization. The system is structured across three levels: the DBMS level (DBApp), the Table level, and the Page level.

## Features

- **Create Tables** – Define tables with custom column names, stored as metadata on disk
- **Insert Records** – Add string array records into pages, with automatic page overflow handling
- **Select Records** – Three modes of selection:
  - Full table scan (`SELECT *`)
  - Condition-based filtering (equivalent to `WHERE` clause)
  - Direct pointer access by page number and record index
- **Operation Tracing** – Every operation is logged with execution time, retrievable as a full trace or last operation only
- **Persistent Storage** – Tables and pages are serialized to disk and reloaded on demand
