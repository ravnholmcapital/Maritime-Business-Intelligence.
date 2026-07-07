# Maritime Business Intelligence & Geospatial Analytics

A professional, dual-project portfolio applying data engineering, spatial database architecture, and financial opex modeling to analyze global maritime supply chain disruptions. 

These projects demonstrate the capability to ingest high-volume telemetry data (130M+ rows), geofence maritime corridors using PostGIS, and translate physical vessel waiting times into direct corporate financial liabilities.

---

## 📂 Project Directory & Interactive Dashboards

| Project Title | Primary Geofence | Technical Stack | Business / Finance Focus | Interactive Dashboard |
| :--- | :---: | :--- | :--- | :---: |
| **1. The 2026 Baltic Shipping Shock** | Skagen Straits (Denmark) | Python, PostGIS, Power BI | Geopolitical War, Fuel Waste, EU ETS Carbon Taxes | [Interactive Report](https://www.novypro.com/) |
| **2. The 2024 US East Coast Port Strike** | Port of Savannah (USA) | Python (Zstd), PostGIS, Power BI | Labor Disputes, Port Dwell, US Demurrage Surcharges | [Interactive Report](https://www.novypro.com/) |

---

## 🏗️ Core Engineering Solutions Demonstrated

To handle high-volume geographic datasets on standard local hardware (developing under a strict 30 GB storage limit), several production-grade database and data engineering practices were implemented:

### 1. In-Memory Streaming ETL
Instead of unzipping and writing massive, multi-gigabyte CSV files directly to disk, the Python ingestion scripts stream the raw data directly from compressed `.zip` and `.zst` archives in RAM. The scripts apply a highly targeted spatial bounding box filter on-the-fly, discarding 80% of irrelevant national data before it ever touches the hard drive, preserving local storage limits.

### 2. Table Partitioning & Spatial Indexing
To prevent database query degradation over millions of records, the parent `master_ais_pings` table is partitioned into non-overlapping temporal ranges. Furthermore, a spatial `GIST` index was constructed on the geometry coordinates, reducing geographic intersection query times from hours to milliseconds.

### 3. Database-First Analytics (Materialized Views)
Instead of executing heavy geospatial containment checks (`ST_Contains`) and time-series sorting (`LAG` window functions) dynamically in Power BI using DAX—which causes visualization lag—all mathematical calculations are executed at the database tier. The results are compiled into a static, indexed **Materialized View** in PostgreSQL, allowing Power BI to import the final aggregated dataset instantly in under 1 second.

---

## 📊 Analytical Project Walkthroughs

### Project 1: The 2026 Baltic Shipping Shock (Europe)
*   **The Disruption:** The outbreak of the 2026 Iran War on February 28, 2026, closed the Strait of Hormuz, spiking marine fuel prices and forcing global shipping lines to reroute around Africa.
*   **The Spatial Logic:** PostGIS was used to geofence the Skagen Anchorage (Danish Straits), tracking vessel waiting times (SOG < 1.0 knot) as they hovered waiting for updated insurance and transit clearances.
*   **The Financial Logic:** Waiting hours were converted to wasted Marine Gas Oil (MGO) tons based on auxiliary engine fuel-burn curves. The model then applied the standard IMO Carbon Conversion Factor (3.206) to calculate exact EU ETS carbon tax exposures (€85/ton of CO2) incurred by the fleet.

### Project 2: The 2024 US East Coast Port Strike (USA)
*   **The Disruption:** A major 3-day labor strike shut down all East and Gulf Coast port terminals, freezing supply chain velocity.
*   **The Spatial Logic:** PostGIS was used to geofence the Tybee Roads Outer Anchorage, tracking container ship dwell times outside the Port of Savannah, Georgia.
*   **The Financial Logic:** Under standard Charter Party agreements, after a 24-hour free laytime window, cargo owners are penalized for delays. This model applied a standard port demurrage rate of $250.00 per day per container based on the vessel's TEU capacity to calculate the exact cash liability shift between the charterer and the shipowner.

---

## 💻 Technical Setup & Execution

### 1. Database Initialization (PostgreSQL/PostGIS)
Locate the `database_schema.sql` file inside either project directory and execute it in your pgAdmin Query Tool to create the partitioned tables, spatial geofences, and indexes.

### 2. Run the Python ETL Ingestion
Install your dependencies and run the streaming script to load the data:
```bash
pip install zstandard psycopg2-binary
python stream_etl.py
