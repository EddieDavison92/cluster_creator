import os
import datetime
import logging
import warnings
import pandas as pd
import pyodbc
from typing import Dict, Set, List

# Suppress UserWarnings (e.g. from pandas)
warnings.simplefilter('ignore', category=UserWarning)

# Set up logging to output to both the terminal and a log file (overwriting each run)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(message)s', '%Y-%m-%d %H:%M:%S')

# File handler for log.txt (overwrites previous log)
file_handler = logging.FileHandler("log.txt", mode='w')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Stream handler for terminal output
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

start_time = datetime.datetime.now()

# Database file paths – please adjust these paths as necessary.
TRANSITIVE_CLOSURE_DB = r"C:\Users\eddie\Downloads\nhs_dmwb_39.0.0_20240925000001\DMWB NHS SNOMED Transitive Closure.mdb"
HISTORY_DB = r"C:\Users\eddie\Downloads\nhs_dmwb_39.0.0_20240925000001\DMWB NHS SNOMED History.mdb"

# Input and output filenames
INPUT_CSV = "Cerner_UK_Clinical_Standard-12-02-2025.csv"
OUTPUT_TABLE_CSV = "snomed_expanded_table.csv"

def connect_to_db(db_path: str) -> pyodbc.Connection:
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + db_path + ';'
    )
    return pyodbc.connect(conn_str)

def load_transitive_closure_efficient(conn_transitive: pyodbc.Connection) -> Dict[str, Set[str]]:
    """
    Efficiently load the entire transitive closure table into a dictionary.
    Each key is a SuperTypeID and the value is the set of SubtypeIDs.
    """
    logging.info("Loading transitive closure table into memory...")
    trans_dict: Dict[str, Set[str]] = {}
    cursor = conn_transitive.cursor()
    cursor.execute("SELECT SuperTypeID, SubtypeID FROM SCTTC")
    batch_size = 10000
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        for row in rows:
            super_code = str(row.SuperTypeID).strip()
            sub_code = str(row.SubtypeID).strip()
            if super_code not in trans_dict:
                trans_dict[super_code] = set()
            trans_dict[super_code].add(sub_code)
    logging.info(f"Loaded transitive closure table with {len(trans_dict)} keys.")
    return trans_dict

def load_history_table_efficient(conn_history: pyodbc.Connection) -> Dict[str, Set[str]]:
    """
    Efficiently load the entire history table into a dictionary.
    Each key is an OLDCUI and the value is the set of NEWCUI values.
    """
    logging.info("Loading history table into memory...")
    history_dict: Dict[str, Set[str]] = {}
    cursor = conn_history.cursor()
    cursor.execute("SELECT OLDCUI, NEWCUI FROM SCTHIST")
    batch_size = 10000
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        for row in rows:
            old_code = str(row.OLDCUI).strip()
            new_code = str(row.NEWCUI).strip()
            if old_code not in history_dict:
                history_dict[old_code] = set()
            history_dict[old_code].add(new_code)
    logging.info(f"Loaded history table with {len(history_dict)} keys.")
    return history_dict

def expand_codes_for_concept(code: str,
                             history_dict: Dict[str, Set[str]],
                             trans_dict: Dict[str, Set[str]]) -> Set[str]:
    """
    Expand a given SNOMED code by including any history replacements and recursively
    finding all child codes (and their history replacements). Both the original and any
    replacement codes are used as expansion points.
    """
    # Start with the original code and its history replacements (if any)
    base_set: Set[str] = {code} | history_dict.get(code, set())
    expanded_codes: Set[str] = set(base_set)
    to_process: Set[str] = set(base_set)

    while to_process:
        new_codes: Set[str] = set()
        for current_code in to_process:
            children = trans_dict.get(current_code, set())
            for child in children:
                child_replacements = history_dict.get(child, set())
                new_codes.add(child)
                new_codes |= child_replacements
        new_codes -= expanded_codes
        if not new_codes:
            break
        expanded_codes |= new_codes
        to_process = new_codes
    return expanded_codes

def process_csv_to_table(input_csv: str, output_csv: str,
                         history_dict: Dict[str, Set[str]],
                         trans_dict: Dict[str, Set[str]]) -> None:
    """
    Read the input CSV, filter for rows where Code System is 'SNOMED CT',
    expand the SNOMED codes for each cluster (using the 'Aliases' field as cluster ID),
    and then write a new CSV file in a compact table format with two columns:
    'Cluster ID' and 'Code'.
    
    Additionally, log for each cluster:
      - the original count (base set count)
      - the number of new codes added (difference between final and original)
      - the final total count.
    """
    logging.info(f"Reading input CSV file '{input_csv}'.")
    df = pd.read_csv(input_csv, dtype=str)
    # Filter for rows where Code System is SNOMED CT
    snomed_mask = df["Code System"].str.upper() == "SNOMED CT"
    df_snomed = df[snomed_mask]

    unique_clusters = df_snomed["Aliases"].unique()
    logging.info(f"Found {len(unique_clusters)} unique cluster IDs to process.")

    output_rows: List[Dict[str, str]] = []
    
    for cluster_id in unique_clusters:
        cluster_rows = df_snomed[df_snomed["Aliases"] == cluster_id]
        base_code = cluster_rows.iloc[0]["Code"]
        if not base_code or pd.isna(base_code):
            logging.warning(f"Cluster '{cluster_id}' has no base code; skipping.")
            continue

        base_code = base_code.strip()
        # Compute base set: the original code plus any immediate history replacements
        base_set: Set[str] = {base_code} | history_dict.get(base_code, set())
        base_count = len(base_set)
        
        try:
            expanded = expand_codes_for_concept(base_code, history_dict, trans_dict)
            final_count = len(expanded)
            added = final_count - base_count
            logging.info(f"Cluster '{cluster_id}': original count = {base_count}, added = {added}, new total = {final_count} codes.")
            for code in sorted(expanded):
                output_rows.append({"Cluster ID": cluster_id, "Code": code})
        except Exception as e:
            logging.error(f"Error expanding cluster '{cluster_id}': {e}")

    df_output = pd.DataFrame(output_rows)
    df_output.to_csv(output_csv, index=False, encoding='utf-8-sig')
    logging.info(f"Expanded table CSV file '{output_csv}' created with {len(df_output)} rows.")

def main() -> None:
    try:
        conn_transitive = connect_to_db(TRANSITIVE_CLOSURE_DB)
        conn_history = connect_to_db(HISTORY_DB)
        logging.info("Database connections opened successfully.")
    except Exception as e:
        logging.error(f"Error connecting to databases: {e}")
        return

    try:
        trans_dict = load_transitive_closure_efficient(conn_transitive)
        history_dict = load_history_table_efficient(conn_history)
    except Exception as e:
        logging.error(f"Error loading tables: {e}")
        conn_transitive.close()
        conn_history.close()
        return

    process_csv_to_table(INPUT_CSV, OUTPUT_TABLE_CSV, history_dict, trans_dict)

    conn_transitive.close()
    conn_history.close()
    logging.info("Database connections closed.")

    end_time = datetime.datetime.now()
    execution_time = end_time - start_time
    total_seconds = execution_time.total_seconds()
    minutes = int(total_seconds // 60)
    seconds = int(total_seconds % 60)
    time_str = f"{minutes} minutes and {seconds} seconds" if minutes > 0 else f"{seconds} seconds"
    logging.info(f"Script executed in {time_str}")

if __name__ == "__main__":
    main()
