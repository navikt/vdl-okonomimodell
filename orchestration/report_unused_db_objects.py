import json
import subprocess

cmd = ["vdc", "waste", "disposal", "--dry-run"]
dbt_config = [
    "--dbt-project-dir",
    "/workspace/dbt",
    "--dbt-profile-dir",
    ".",
]
check_schemas = [
    "--schema",
    "okonomimodell.stages",
    "--schema",
    "okonomimodell.intermediates",
    "--schema",
    "okonomimodell.marts",
    "--schema",
    "okonomimodell.oebs",
]
ignore_tables: list[str] = []
output = subprocess.run(
    cmd + dbt_config + check_schemas + ignore_tables,
    capture_output=True,
    text=True,
)
if output.returncode != 0:
    print("Error running command:", output.stderr)
    print("Command output:", output.stdout)
    exit(1)

formated_output = f"\n*Ubrukte objekter i vdl-okonomimodell*\n```\n{output.stdout}```"
output_json_dump = json.dumps({"dump": formated_output}, indent=4)
with open("/airflow/xcom/return.json", "w") as f:
    f.write(output_json_dump)
