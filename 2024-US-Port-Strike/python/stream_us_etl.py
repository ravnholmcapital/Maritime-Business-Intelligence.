#!/usr/bin/env python3
"""
US ETL Pipeline: Streams ZST, ZIP, TGZ, and RAR files directly in RAM.
Dynamically standardizes US Coast Guard column headers on-the-fly.
"""
import os
import re
import zipfile
import csv
import io
import time
import contextlib
import psycopg2
from psycopg2.extras import execute_values
import zstandard as zstd

# Database Connection (Passwordless local trust setup)
DB_CONFIG = {
    "dbname": "maritime_db",
    "user": "postgres",
    "host": "localhost",
    "port": "5432"
}

# Directories (Updated with SIS_PROJECT folder path)
DATA_DIR = r"data"

# Spatial Bounding Box: US Savannah Coastal limits
LAT_MIN, LAT_MAX = 31.5, 32.5
LON_MIN, LON_MAX = -81.5, -80.5


def clean_headers(headers):
    # Maps US Coast Guard headers to our standard schema
    clean = []
    for h in headers:
        h_clean = h.strip().lower().replace('#', '').replace(' ', '_').strip()
        if h_clean in ('base_date_time', 'basetime'):
            h_clean = 'timestamp'
        elif h_clean == 'lat':
            h_clean = 'latitude'
        elif h_clean == 'lon':
            h_clean = 'longitude'
        clean.append(h_clean)
    return clean


@contextlib.contextmanager
def open_compressed_csv(file_path):
    lower_path = file_path.lower()

    # 1. PROCESS ZIP FILES
    if lower_path.endswith('.zip'):
        with zipfile.ZipFile(file_path, 'r') as z:
            csv_filename = [n for n in z.namelist() if n.lower().endswith('.csv')][0]
            with z.open(csv_filename, 'r') as f:
                yield io.TextIOWrapper(f, encoding='utf-8')

    # 2. PROCESS ZSTANDARD (ZST) FILES
    elif lower_path.endswith('.zst'):
        dctx = zstd.ZstdDecompressor()
        with open(file_path, 'rb') as fh:
            with dctx.stream_reader(fh) as reader:
                yield io.TextIOWrapper(reader, encoding='utf-8')

    # 3. PROCESS TGZ / TAR.GZ FILES
    elif lower_path.endswith('.tgz') or lower_path.endswith('.tar.gz'):
        import tarfile
        with tarfile.open(file_path, 'r:gz') as tar:
            csv_filename = [m.name for m in tar.getmembers() if m.name.lower().endswith('.csv')][0]
            f = tar.extractfile(csv_filename)
            yield io.TextIOWrapper(f, encoding='utf-8')

    # 4. PROCESS RAR FILES
    elif lower_path.endswith('.rar'):
        import rarfile
        with rarfile.RarFile(file_path, 'r') as rf:
            csv_filename = [n for n in rf.namelist() if n.lower().endswith('.csv')][0]
            with rf.open(csv_filename, 'r') as f:
                yield io.TextIOWrapper(f, encoding='utf-8')
    else:
        # Fallback for uncompressed CSVs
        with open(file_path, 'r', encoding='utf-8') as f:
            yield f


def process_file(file_name):
    file_path = os.path.join(DATA_DIR, file_name)
    start_time = time.time()

    # Extract expected Year, Month, and Day from filename (e.g., "ais-2024-10-01.csv.zst")
    match = re.search(r'2024-\d{2}-\d{2}', file_name)
    if not match:
        print(f"    [!] Skipping {file_name}: Date pattern 'YYYY-MM-DD' not found in filename.")
        return 0

    expected_y, expected_m, expected_d = match.group(0).split('-')

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SET TIME ZONE 'UTC';")
    cursor.execute("SET DateStyle TO 'DMY';")

    inserted_count = 0
    total_rows_processed = 0
    pings_to_insert = []

    print(f"\n--> Streaming: {file_name}")

    with open_compressed_csv(file_path) as text_stream:
        csv_reader = csv.DictReader(text_stream)
        csv_reader.fieldnames = clean_headers(csv_reader.fieldnames)

        for row in csv_reader:
            total_rows_processed += 1
            try:
                lat = float(row['latitude'])
                lon = float(row['longitude'])
                sog = float(row['sog'])
                mmsi = int(row['mmsi'])
                timestamp = row['timestamp']

                # 1. Spatial Filter: Only keep pings inside the Savannah bounding box
                if (LAT_MIN <= lat <= LAT_MAX) and (LON_MIN <= lon <= LON_MAX):

                    # 2. State Filter: Only keep stationary/anchored (SOG < 1.0) or active transit (SOG > 5.0)
                    if sog < 1.0 or sog > 5.0:
                        # Parse US DateStyle formats (handles "YYYY-MM-DDTHH:MM:SS" or "YYYY-MM-DD HH:MM:SS")
                        parts = timestamp.split('T' if 'T' in timestamp else ' ')
                        y, m, d = parts[0].split('-')
                        time_part = parts[1].replace('Z', '')

                        # Temporal Quality Filter: Discard corrupted dates
                        if y != expected_y or m != expected_m or d != expected_d:
                            continue

                        utc_timestamp = f"{y}-{m}-{d} {time_part}+00"

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
                        "INSERT INTO us_ais_pings_2024 (mmsi, ping_timestamp, latitude, longitude, sog, geom) VALUES %s",
                        pings_to_insert,
                        template="(%s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326))",
                        page_size=10000
                    )
                    pings_to_insert = []

            except (ValueError, KeyError, TypeError, IndexError):
                continue

        # Insert remaining rows
        if pings_to_insert:
            execute_values(
                cursor,
                "INSERT INTO us_ais_pings_2024 (mmsi, ping_timestamp, latitude, longitude, sog, geom) VALUES %s",
                pings_to_insert,
                template="(%s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326))",
                page_size=10000
            )
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
        print(f"[-] Error: Directory '{DATA_DIR}' does not exist.")
        return

    # Check if there are any .rar files to process
    if any(f.lower().endswith('.rar') for f in os.listdir(DATA_DIR)):
        try:
            import rarfile
        except ImportError:
            print("[-] Error: 'rarfile' library is required to process .rar files.")
            print("[i] Run: pip install rarfile")
            return

    # Find all daily compressed files in your directory
    supported_exts = ('.zip', '.zst', '.tgz', '.tar.gz', '.rar')
    compressed_files = sorted([f for f in os.listdir(DATA_DIR) if f.lower().endswith(supported_exts)])

    if not compressed_files:
        print(f"[-] No raw files found in {DATA_DIR} matching extensions {supported_exts}.")
        return

    print("=" * 60)
    print("   LAUNCHING US PORT STRIKE ETL STREAMING PIPELINE")
    print("=" * 60)
    print(f"   Target Directory: {DATA_DIR}")
    print("=" * 60)

    total_saved = 0
    global_start_time = time.time()

    for file_name in compressed_files:
        try:
            saved_count = process_file(file_name)
            total_saved += saved_count
        except Exception as e:
            print(f"    [!] Error processing {file_name}: {e}")
            continue

    global_elapsed = time.time() - global_start_time
    print("\n" + "=" * 60)
    print("   ETL PIPELINE RUN COMPLETE")
    print("=" * 60)
    print(f"   Total Daily Files Processed: {len(compressed_files)}")
    print(f"   Total Spatial Records Saved: {total_saved:,}")
    print(f"   Total Execution Time:        {global_elapsed:.2f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()