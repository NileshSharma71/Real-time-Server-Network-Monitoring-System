# 🌍 Real‑Time Global Server Monitoring System

> A **Lambda Architecture** big‑data pipeline built with **Apache Spark**, **Python**, and **Streamlit**.

**Note:** Ensure your mermaid diagram image is saved as `pipeline.png` in your repo.

---

## 🚀 Project Overview

This repository contains an end‑to‑end pipeline to monitor server health (CPU, Temperature, Power Usage) across distributed global data centers. The design follows a **Lambda Architecture** that separates processing into a **Batch Layer** (history / accuracy) and a **Speed Layer** (low‑latency real‑time updates).

Key capabilities:

* Ingests heterogeneous data from **CSV (India)**, **JSON (USA)** and **Parquet (Germany)** sources.
* Handles schema evolution and unit normalization (e.g., Fahrenheit/Kelvin → Celsius).
* Streams live telemetry via a TCP socket (port `9988`) using Spark Structured Streaming.
* Dual sink strategy: **PostgreSQL** (hot path for dashboarding) and **JSON archives** (cold path for audits).
* Streamlit dashboard that auto‑refreshes, visualizes trends, and raises anomaly alerts (e.g., servers > 90°C).

---

## 🔑 Key Features

1. **Heterogeneous Data Sources** — Bulk historical files in CSV, JSON, and Parquet.
2. **Real‑Time Streaming** — Spark Structured Streaming consumes live telemetry from a TCP socket.
3. **Lambda Architecture** — Batch and Speed layers run together to ensure both historic accuracy and real‑time responsiveness.
4. **Dual‑Sink Storage**

   * **Hot Path:** Writes transformed data to PostgreSQL for immediate dashboard queries.
   * **Cold Path:** Stores raw/archived payloads as JSON partitions (e.g. `data/live_data/stream_run_...`) for audits and replay.
5. **Schema Evolution & Normalization** — Automatically unifies schemas and converts units; gracefully handles missing sensor fields.
6. **Live Dashboard** — Streamlit UI with charts, histograms, data volume metrics, anomaly alerts, and playback speed controls.

---

## 🏗 Architecture Flow

### 1. Source Layer (Data Generator)

* Simulates a global network across **3 regions**: India, USA, Germany.
* Produces bulk historical files (`.csv`, `.json`, `.parquet`).
* Starts a socket server on **port 9988** to stream live telemetry packets.

### 2. Processing Layer (Apache Spark)

* **Batch Layer** (`etl_pipeline.py`) — Reads historical datasets, normalizes fields/units, unions schemas, and seeds the PostgreSQL baseline.
* **Speed Layer** (`streaming_pipeline.py`) — Listens to the socket, applies transformations, appends hot/cold sinks, and handles realtime normalization.

### 3. Storage Layer

* **PostgreSQL** — Hot store used by the Streamlit dashboard.
* **JSON Archive** — Cold storage that keeps raw payloads in timestamped partitions.

### 4. Serving Layer

* Streamlit dashboard queries PostgreSQL frequently (configurable; default: 1 second) to render live charts, detect anomalies (e.g., overheating), and show regional metrics.

---

## 🛠 Tech Stack

* **Language:** Python 3.x
* **Big Data Engine:** Apache Spark (PySpark)
* **Streaming:** Spark Structured Streaming
* **Database:** PostgreSQL
* **Visualization:** Streamlit
* **Infrastructure:** Hadoop WinUtils (Windows compatibility)

---

## ⚡ Prerequisites

Make sure you have the following installed and running:

* Java 17 (OpenJDK)
* Python 3.10+
* PostgreSQL running locally on port `5432`

---

## 🧩 How to Run (One‑Click)

**Clone the repository**

```bash
git clone https://github.com/NileshSharma71/Real-time-Server-Network-Monitoring-System.git
cd Real-time-Server-Network-Monitoring-System
```

**Install Python dependencies**

```bash
pip install -r requirements.txt
```

**Setup PostgreSQL**

Create a database named `bigdata_db` in your PostgreSQL server. Example (psql):

```sql
CREATE DATABASE bigdata_db;
```

**Launch the system (one‑click)**

Double‑click `start_project.bat` (Windows) — this script automates the workflow by:

1. Starting the Data Generator (historical files + socket live stream).
2. Running the Batch ETL to load historical data into the DB.
3. Starting the Streaming Pipeline to process live telemetry.
4. Launching the Streamlit Dashboard in your browser.

> The script expects PostgreSQL to be reachable on `localhost:5432` and Python/Java to be available in your PATH.

---

## 📊 Dashboard Preview

The dashboard provides:

* Live CPU load per region
* Temperature distribution histograms
* Data volume tracking
* Anomaly alerts (e.g., servers exceeding 90°C)
* Slide controls to speed up / slow down the live feed

![Project Pipeline](pipeline2.png)

---

## 📝 Notes & Tips

* Keep the mermaid diagram saved as `pipeline.png` in the repository (used for architecture docs).
* Cold archives are stored under `data/live_data/` with `stream_run_*` timestamped folders for replay and audit.
* Tweak the Streamlit refresh interval and Spark checkpoint locations in the pipeline config files if needed.

---

## Contributing

Contributions, bug reports, and feature requests are welcome. Open an issue or submit a pull request.

---

## License

Include your preferred license here (e.g. MIT) if you want to make this project open source.

---

*Created with care — good luck building your real‑time monitoring system!*


![Project Pipeline](pipeline2.png)
