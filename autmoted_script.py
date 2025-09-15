import argparse
import shutil
import subprocess
import sys
from pathlib import Path
from datetime import datetime
import sqlparse

def check_cli():
    if shutil.which("databricks") is None:
        sys.exit("ERROR: 'databricks' CLI not found in PATH. Install/configure it and try again.")

def ensure_dirs(folder: Path):
    folder.mkdir(parents=True, exist_ok=True)

def run_cmd(cmd_str: str, title: str):
    print(f"\n=== {title} ===")
    print("Command:", cmd_str)
    try:
        result = subprocess.run(cmd_str, shell=True, timeout=600)
        if result.returncode != 0:
            sys.exit(f"{title} failed with exit code {result.returncode}")
    except subprocess.TimeoutExpired:
        sys.exit(f"{title} timed out after 600 seconds")

def process_sql_files(converted_folder: Path, notebooks_folder: Path):
    # ✅ Only set Final_Formatted ONCE
    final_folder = converted_folder.parent / "Final_Formatted"
    ensure_dirs(final_folder)
    ensure_dirs(notebooks_folder)

    for sql_file in converted_folder.glob("*.sql"):
        with open(sql_file, "r", encoding="utf-8") as f:
            sql_content = f.read()

        # 🔄 Perform replacements
        sql_content = sql_content.replace("edw.", "edp_datawarehouse_prd.")
        sql_content = sql_content.replace("isdelete = 0", "etl_is_active = 1")
        sql_content = sql_content.replace("IsDelete = 0", "etl_is_active = 1")
        sql_content = sql_content.replace("isdelete=0", "etl_is_active = 1")
        sql_content = sql_content.replace("finacle.", "edp_bfil_prod.finacle.")

        # 🎨 Format SQL nicely
        sql_content = sqlparse.format(sql_content, reindent=True, keyword_case="upper")

        # 1️⃣ Save to Final_Formatted folder
        final_file = final_folder / sql_file.name
        with open(final_file, "w", encoding="utf-8") as f:
            f.write(sql_content)

        # 2️⃣ Create Databricks notebook (.py) file
        notebook_file = notebooks_folder / (sql_file.stem + ".py")
        with open(notebook_file, "w", encoding="utf-8") as f:
            f.write("# Databricks notebook source\n")
            f.write(f'"""\nAuto-generated from {sql_file.name}\n"""\n\n')
            f.write('sql_query = """\n')
            f.write(sql_content)
            f.write('\n"""\n')
            f.write("display(spark.sql(sql_query))\n")

        # 3️⃣ Upload notebook to Databricks (✅ Fixed CLI command)
        upload_cmd = (
            f'databricks workspace import '
            f'--file "{notebook_file}" '
            f'"/Shared/{notebook_file.name}" '
            f'--language PYTHON --overwrite'
        )
        run_cmd(upload_cmd, f"Upload Notebook {notebook_file.name}")

def main():
    parser = argparse.ArgumentParser(description="Run Lakebridge Analyze and Convert (Transpile) commands.")
    parser.add_argument("--source-path", help="Folder (or file) used for analyze/transpile --source-directory")
    parser.add_argument("--target-path", help="Folder where analyzer report and converted code will be saved")
    parser.add_argument("--dialect", help="Source dialect/tech (e.g., synapse, oracle, teradata)")
    parser.add_argument("--profile", help="Optional Databricks CLI profile name (maps to -p/--profile)")
    parser.add_argument("--debug", action="store_true", help="Enable Lakebridge debug logging (--debug)")
    args = parser.parse_args()

    source_path = args.source_path or input('Enter source path (folder or file): ').strip('"').strip()
    target_path = args.target_path or input('Enter target folder: ').strip('"').strip()
    dialect = args.dialect or input('Enter source dialect/tech (e.g., synapse, oracle, teradata): ').strip()

    target_folder = Path(target_path)
    ensure_dirs(target_folder)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = target_folder / f"lakebridge_analysis_{ts}.xlsx"
    ensure_dirs(report_file.parent)

    check_cli()
    if not Path(source_path).exists():
        sys.exit(f"ERROR: source path not found: {source_path}")

    global_flags = []
    if args.profile:
        global_flags += ["-p", args.profile]
    if args.debug:
        global_flags += ["--debug"]

    # 1️⃣ Run Analyzer
    analyze_cmd_parts = [
        "databricks labs lakebridge analyze",
        f'--source-directory "{source_path}"',
        f'--report-file "{report_file}"',
        f'--source-tech {dialect}',
    ] + global_flags
    run_cmd(" ".join(analyze_cmd_parts), "Lakebridge Analyze")

    # 2️⃣ Run Converter (Transpile)
    converted_folder = target_folder / "Converted_Code"
    ensure_dirs(converted_folder)

    transpile_cmd_parts = [
        "databricks labs lakebridge transpile",
        f'--input-source "{source_path}"',
        f'--source-dialect {dialect}',
        f'--output-folder "{converted_folder}"',
    ] + global_flags
    run_cmd(" ".join(transpile_cmd_parts), "Lakebridge Transpile")

    # 3️⃣ Post-process SQL + Generate Notebooks
    notebooks_folder = target_folder / "Databricks_Notebooks"
    process_sql_files(converted_folder, notebooks_folder)

    print("\nAll done ✅")
    print(f"Analyzer report saved at: {report_file}")
    print(f"Converted scripts saved at: {converted_folder}")
    print(f"Final formatted scripts saved at: {converted_folder.parent / 'Final_Formatted'}")
    print(f"Databricks notebooks saved at: {notebooks_folder} and uploaded to /Shared in workspace")

if __name__ == "__main__":
    main()
