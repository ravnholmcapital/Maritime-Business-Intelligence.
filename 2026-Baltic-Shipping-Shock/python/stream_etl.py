#!/usr/bin/env python3
"""
ETL Pipeline: Streams compressed AIS data from ZIP to PostgreSQL.
Applies spatial and state filtering on-the-fly to manage storage limits.
Optimized Version: Uses psycopg2.extras.execute_values for 100x bulk insert speed.
"""
import os
import zipfile
import csv
import io
import time
import psycopg2
from psycopg2.extras import execute_values  # High-speed bulk loader

# Database Connection Parameters (Passwordless local trust setup)
DB_CONFIG = {
    "dbname": "maritime_db",
    "user": "postgres",
    "host": "localhost",
    "port": "5432"
}

# Directories (Danish data folder with leading space)
DATA_DIR = r"E:\ANNOTATION\ data"

# Spatial Bounding Box: Danish Straits
LAT_MIN, LAT_MAX = 54.5, 58.0
LON_MIN, LON_MAX = 9.5, 13.0


def process_daily_zip(zip_name):
    zip_path = os.path.join(DATA_DIR, zip_name)
    start_time = time.time()

    parts = zip_name.split('-')
    expected_y = parts[1]
    expected_m = parts[2]

    # Establish local database connection
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Force connection session to UTC and set DateStyle
    cursor.execute("SET TIME ZONE 'UTC';")
    cursor.execute("SET DateStyle TO 'DMY';")

    inserted_count = 0
    total_rows_processed = 0

    print(f"\n--> Streaming: {zip_name}")

    with zipfile.ZipFile(zip_path, 'r') as z:
        csv_filename = [f for f in z.namelist() if f.lower().endswith('.csv')][0]

        with z.open(csv_filename, 'r') as f:
            text_stream = io.TextIOWrapper(f, encoding='utf-8')
            csv_reader = csv.DictReader(text_stream)

            # Standardize headers to lowercase
            csv_reader.fieldnames = [
                h.strip().lower().replace('#', '').strip()
                for h in csv_reader.fieldnames
            ]

            pings_to_insert = []

            for row in csv_reader:
                total_rows_processed += 1
                try:
                    lat = float(row['latitude'])
                    lon = float(row['longitude'])
                    sog = float(row['sog'])
                    mmsi = int(row['mmsi'])
                    timestamp = row['timestamp']

                    # 1. Spatial Filter: Only keep pings inside the Danish Straits
                    if (LAT_MIN <= lat <= LAT_MAX) and (LON_MIN <= lon <= LON_MAX):

                        # 2. State Filter: Only keep stationary/anchored (SOG < 1.0) or active transit (SOG > 5.0)
                        if sog < 1.0 or sog > 5.0:
                            parts = timestamp.split(' ')
                            d, m, y = parts[0].split('/')

                            # Temporal Quality Filter: Discard corrupted future/past dates
                            if y != expected_y or m != expected_m:
                                continue

                            utc_timestamp = f"{y}-{m}-{d} {parts[1]}+00"

                            pings_to_insert.append((
                                mmsi,
                                utc_timestamp,
                                lat,
                                lon,
                                sog,
                                f"POINT({lon} {lat})"
                            ))
                            inserted_count += 1

                    # High-speed bulk insert every 30,000 rows
                    if len(pings_to_insert) >= 30000:
                        execute_values(
                            cursor,
                            "INSERT INTO master_ais_pings (mmsi, ping_timestamp, latitude, longitude, sog, geom) VALUES %s",
                            pings_to_insert,
                            template="(%s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326))",
                            page_size=10000
                        )
                        # We do not commit yet to save disk write overhead
                        pings_to_insert = []

                except (ValueError, KeyError, TypeError, IndexError):
                    continue

            # Insert any remaining records in the final batch
            if pings_to_insert:
                execute_values(
                    cursor,
                    "INSERT INTO master_ais_pings (mmsi, ping_timestamp, latitude, longitude, sog, geom) VALUES %s",
                    pings_to_insert,
                    template="(%s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326))",
                    page_size=10000
                )

            # Commit the entire daily file once at the very end to minimize disk writes
            conn.commit()

    cursor.close()
    conn.close()

    elapsed = time.time() - start_time
    print(f"    Processed: {total_rows_processed:,} rows")
    print(f"    Saved to DB: {inserted_count:,} rows")
    print(f"    Time Elapsed: {elapsed:.2f}s")
    return inserted_count


def main():
    if not os.path.exists(DATA_DIR):
        print(f"[-] Error: The directory '{DATA_DIR}' does not exist.")
        return

    zip_files = sorted([f for f in os.listdir(DATA_DIR) if f.lower().endswith('.zip')])

    if not zip_files:
        print(f"[-] No ZIP files found in {DATA_DIR}. Verify the path.")
        return

    print("=" * 60)
    print("   LAUNCHING MARITIME STREAMING ETL PIPELINE")
    print("=" * 60)
    print(f"   Target Directory: {DATA_DIR}")
    print("=" * 60)

    total_saved = 0
    global_start_time = time.time()

    for zip_file in zip_files:
        try:
            saved_count = process_daily_zip(zip_file)
            total_saved += saved_count
        except Exception as e:
            print(f"    [!] Error processing {zip_file}: {e}")
            continue

    global_elapsed = time.time() - global_start_time
    print("\n" + "=" * 60)
    print("   ETL PIPELINE RUN COMPLETE")
    print("=" * 60)
    print(f"   Total Daily Files Processed: {len(zip_files)}")
    print(f"   Total Spatial Records Saved: {total_saved:,}")
    print(f"   Total Execution Time:        {global_elapsed:.2f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()