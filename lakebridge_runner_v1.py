import argparse
import shutil
import subprocess
import sys
import logging
import yaml
from pathlib import Path
from datetime import datetime
import sqlparse
import csv
import urllib.request  # >>> Added

# -------------------------
# Setup Logging (file only)
# -------------------------
def setup_logging(metadata_folder: Path):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = metadata_folder / f"lakebridge_run_{ts}.txt"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    logging.info(f"Logging initialized. Log file: {log_file}")
    return log_file

# -------------------------
# Utility functions
# -------------------------
def check_cli():
    if shutil.which("databricks") is None:
        sys.exit("ERROR: 'databricks' CLI not found in PATH. Install/configure it and try again.")

def ensure_dirs(folder: Path):
    folder.mkdir(parents=True, exist_ok=True)

def run_cmd(cmd_str: str, title: str, log_file=None, ignore_failure=False):
    print(f"\n=== {title} ===")
    print("Command:", cmd_str)
    try:
        result = subprocess.run(cmd_str, shell=True, timeout=600)
        if result.returncode != 0:
            msg = f"{title} failed with exit code {result.returncode}"
            if log_file:
                with open(log_file, "a", encoding="utf-8") as f:
                    f.write(msg + "\n")
            if not ignore_failure:
                sys.exit(msg)
            return False
        return True
    except subprocess.TimeoutExpired:
        msg = f"{title} timed out after 600 seconds"
        if log_file:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        if not ignore_failure:
            sys.exit(msg)
        return False

def validate_input_folder(source_path: Path):
    if not source_path.exists():
        sys.exit(f"ERROR: source path not found: {source_path}")
    if not any(source_path.glob("*.sql")):
        print(f"WARNING: No .sql files found in {source_path}")

# -------------------------
# SQL Post-processing + notebooks
# -------------------------
def process_sql_files(converted_folder: Path, notebooks_folder: Path, metadata_folder: Path):
    final_folder = converted_folder.parent / "Final_Formatted"
    ensure_dirs(final_folder)
    ensure_dirs(notebooks_folder)
    
    summary = []

    print("\nPost-processing SQL and generating notebooks started...")
    for sql_file in converted_folder.glob("*.sql"):
        status = "Succeeded"
        try:
            with open(sql_file, "r", encoding="utf-8", errors="replace") as f:
                sql_content = f.read()
            
            # 🔄 Replace patterns
            sql_content = sql_content.replace("edw.", "edp_datawarehouse_prd.")
            sql_content = sql_content.replace("isdelete = 0", "etl_is_active = 1")
            sql_content = sql_content.replace("IsDelete = 0", "etl_is_active = 1")
            sql_content = sql_content.replace("isdelete=0", "etl_is_active = 1")
            sql_content = sql_content.replace("finacle.", "edp_bfil_prod.finacle.")

            # 🎨 Format SQL
            sql_content = sqlparse.format(sql_content, reindent=True, keyword_case="upper")

            # Save to Final_Formatted
            final_file = final_folder / sql_file.name
            with open(final_file, "w", encoding="utf-8") as f:
                f.write(sql_content)

            # Create Databricks notebook (.py)
            notebook_file = notebooks_folder / (sql_file.stem + ".py")
            with open(notebook_file, "w", encoding="utf-8") as f:
                f.write("# Databricks notebook source\n")
                f.write(f'"""\nAuto-generated from {sql_file.name}\n"""\n\n')
                f.write('sql_query = """\n')
                f.write(sql_content)
                f.write('\n"""\n')
                f.write("display(spark.sql(sql_query))\n")

            # Upload notebook to Databricks (ignore failures here)
            upload_cmd = (
                f'databricks workspace import '
                f'--file "{notebook_file}" '
                f'"/Shared/{notebook_file.name}" '
                f'--language PYTHON --overwrite'
            )
            run_cmd(upload_cmd, f"Upload Notebook {notebook_file.name}", log_file=metadata_folder / f"lakebridge_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt", ignore_failure=True)

        except Exception as e:
            status = f"Failed: {e}"
            logging.error(f"Error processing {sql_file.name}: {e}")
        
        summary.append((sql_file.name, status))

    return summary

# -------------------------
# Main function
# -------------------------
def main():
    parser = argparse.ArgumentParser(description="Run Lakebridge Analyze and Convert (Transpile) commands.")
    parser.add_argument("--config", default="config.yaml", help="Path to YAML config file")  # >>> changed default
    args = parser.parse_args()

    # >>> Added: auto-download config.yaml if not found
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Config file {config_path} not found. Downloading from GitHub...")
        url = "https://raw.githubusercontent.com/satyamhtek/LakeBridge/main/config.yaml"
        urllib.request.urlretrieve(url, config_path)
        print(f"Downloaded default config to {config_path}")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    source_path = Path(config["source_path"])
    target_path = Path(config["target_path"])
    dialect = config["dialect"]
    profile = config.get("profile")
    debug = config.get("debug", False)
    run_validation = config.get("run_validation", True)

    ts_folder = datetime.now().strftime("%Y%m%d")
    metadata_folder = target_path / "metadata" / ts_folder
    ensure_dirs(metadata_folder)
    log_file = setup_logging(metadata_folder)

    print("\nLakebridge script started\n")
    check_cli()
    if run_validation:
        validate_input_folder(source_path)

    analyzer_output_folder = target_path / "analyzer_output"
    ensure_dirs(analyzer_output_folder)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    analyzer_report_file = analyzer_output_folder / f"lakebridge_analysis_{ts}.xlsx"

    global_flags = []
    if profile:
        global_flags += ["-p", profile]
    if debug:
        global_flags += ["--debug"]

    # -------------------------
    # 1️⃣ Analyzer (single run, track per file)
    # -------------------------
    analyzer_status_dict = {}
    try:
        analyze_cmd_parts = [
            "databricks labs lakebridge analyze",
            f'--source-directory "{source_path}"',
            f'--report-file "{analyzer_report_file}"',
            f'--source-tech {dialect}',
        ] + global_flags
        run_cmd(" ".join(analyze_cmd_parts), "Lakebridge Analyze", log_file=log_file)
        for sql_file in source_path.glob("*.sql"):
            analyzer_status_dict[sql_file.name] = "Success"
    except Exception as e:
        logging.error(f"Analyzer failed: {e}")
        for sql_file in source_path.glob("*.sql"):
            analyzer_status_dict[sql_file.name] = "Failed"

    # -------------------------
    # 2️⃣ Transpiler (file by file)
    # -------------------------
    converted_folder = target_path / "Converted_Code"
    ensure_dirs(converted_folder)

    transpile_status_dict = {}
    print("\nStarting transpile per SQL file...")
    for sql_file in source_path.glob("*.sql"):
        try:
            transpile_cmd_parts = [
                "databricks labs lakebridge transpile",
                f'--input-source "{sql_file}"',
                f'--source-dialect {dialect.lower()}',
                f'--output-folder "{converted_folder}"',
            ] + global_flags
            success = run_cmd(" ".join(transpile_cmd_parts), f"Transpile {sql_file.name}", log_file=log_file, ignore_failure=True)
            transpile_status_dict[sql_file.name] = "Success" if success else "Failed"
        except Exception as e:
            logging.error(f"Transpile failed for {sql_file.name}: {e}")
            transpile_status_dict[sql_file.name] = "Failed"

    # -------------------------
    # 3️⃣ Post-process SQL + generate notebooks
    # -------------------------
    notebooks_folder = target_path / "Databricks_Notebooks"
    post_process_summary = process_sql_files(converted_folder, notebooks_folder, metadata_folder)
    # post_process_summary has only 'Succeeded/Failed' for post-processing step

    # -------------------------
    # 4️⃣ Write combined CSV summary
    # -------------------------
    summary_file = metadata_folder / f"sql_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(summary_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Script Name", "Analyzer Status", "Transpile Status", "Post-process Status"])
        all_files = set(list(analyzer_status_dict.keys()) + list(transpile_status_dict.keys()))
        post_process_dict = dict(post_process_summary)
        for file_name in all_files:
            writer.writerow([
                file_name,
                analyzer_status_dict.get(file_name, "Failed"),
                transpile_status_dict.get(file_name, "Failed"),
                post_process_dict.get(file_name, "Failed")
            ])
    print(f"\nAll tasks completed. Summary CSV saved at {summary_file}")

    sys.exit(0)

if __name__ == "__main__":
    main()

