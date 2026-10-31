#!/usr/bin/env python3
"""Capture existing SFN settings and render a Git-safe configuration template.
No AWS calls; capture AWS state definition separately with aws CLI.
"""
import argparse
import json
import os
import re
from pathlib import Path

KEYS = (
    "CATEGORY_LAMBDA_ARN", "VENUE_LAMBDA_ARN",
    "USERS_GLUE_JOB", "EVENTS_GLUE_JOB", "LISTINGS_GLUE_JOB", "SALES_GLUE_JOB",
    "REDSHIFT_WORKGROUP", "REDSHIFT_DATABASE", "S3_BUCKET", "REDSHIFT_COPY_ROLE_ARN",
    "SNS_TOPIC_ARN", "WAIT_STAGING_SECONDS", "WAIT_DIMENSIONS_SECONDS", "WAIT_FACTS_SECONDS", "WORKFLOW_TIMEOUT_SECONDS",
)


def extract(state_machine):
    a = state_machine["States"]
    branch_map = {b["StartAt"]: b["States"][b["StartAt"]]["Parameters"]
                  for b in a["ParallelBronzeToSilver"]["Branches"]}
    params = a["LoadStaging"]["Parameters"]
    sql = params["Sql"]
    match = re.fullmatch(r"CALL (?:public\.)?sp_load_(?:staging|warehouse)\('([^']+)', '([^']+)'(?:, CAST\(:load_date AS DATE\))?\)", sql)
    if not match:
        raise ValueError("Unrecognized staging CALL signature; review current SQL manually")
    return {
        "CATEGORY_LAMBDA_ARN": branch_map["CategoryETL"]["FunctionName"],
        "VENUE_LAMBDA_ARN": branch_map["VenueETL"]["FunctionName"],
        "USERS_GLUE_JOB": branch_map["UsersETL"]["JobName"],
        "EVENTS_GLUE_JOB": branch_map["EventsETL"]["JobName"],
        "LISTINGS_GLUE_JOB": branch_map["ListingsETL"]["JobName"],
        "SALES_GLUE_JOB": branch_map["SalesETL"]["JobName"],
        "REDSHIFT_WORKGROUP": params["WorkgroupName"],
        "REDSHIFT_DATABASE": params["Database"],
        "S3_BUCKET": match.group(1),
        "REDSHIFT_COPY_ROLE_ARN": match.group(2),
        "SNS_TOPIC_ARN": a["NotifySuccess"]["Parameters"]["TopicArn"],
        "WAIT_STAGING_SECONDS": a["WaitForStaging"]["Seconds"],
        "WAIT_DIMENSIONS_SECONDS": a.get("WaitForDimensions", {"Seconds": 5})["Seconds"],
        "WAIT_FACTS_SECONDS": a.get("WaitForFacts", {"Seconds": 5})["Seconds"],
        "WORKFLOW_TIMEOUT_SECONDS": state_machine["TimeoutSeconds"],
    }


def validate_config(config):
    missing = sorted(set(KEYS) - set(config))
    if missing:
        raise ValueError(f"Missing config keys: {missing}")
    for key in KEYS:
        value = config[key]
        if key.endswith("SECONDS"):
            if type(value) is not int or not 1 <= value <= 86400:
                raise ValueError(f"{key} must be an integer within 1..86400")
        elif not isinstance(value, str) or not value or "REPLACE_WITH" in value:
            raise ValueError(f"{key} must contain a real nonempty value")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{2,62}", config["S3_BUCKET"]):
        raise ValueError("Invalid S3 bucket format")
    if not re.fullmatch(r"arn:aws[a-z-]*:iam::\d{12}:role/[A-Za-z0-9+=,.@_/-]+", config["REDSHIFT_COPY_ROLE_ARN"]):
        raise ValueError("COPY role ARN invalid")
    for key in ("CATEGORY_LAMBDA_ARN", "VENUE_LAMBDA_ARN"):
        if not re.fullmatch(r"arn:aws[a-z-]*:lambda:[a-z0-9-]+:\d{12}:function:[A-Za-z0-9-_]+(?:[:][A-Za-z0-9-_]+)?", config[key]):
            raise ValueError(f"Invalid {key}")
    if not re.fullmatch(r"arn:aws[a-z-]*:sns:[a-z0-9-]+:\d{12}:[A-Za-z0-9_-]+", config["SNS_TOPIC_ARN"]):
        raise ValueError("Invalid SNS topic ARN")
    for key in ("USERS_GLUE_JOB", "EVENTS_GLUE_JOB", "LISTINGS_GLUE_JOB", "SALES_GLUE_JOB"):
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,255}", config[key]):
            raise ValueError(f"Invalid {key}")
    for key in ("REDSHIFT_DATABASE", "REDSHIFT_WORKGROUP"):
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,126}", config[key]):
            raise ValueError(f"Invalid {key}")
    # Avoid SQL injection when embedding bucket and IAM role in the staged procedure CALL.
    # The strict S3/ARN allowlists above exclude single quotes and SQL tokens.
    return config


def render(template, config):
    validate_config(config)
    pattern = re.compile(r"\{\{([A-Z0-9_]+)\}\}")
    def visit(obj):
        if isinstance(obj, dict):
            return {k: visit(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [visit(v) for v in obj]
        if isinstance(obj, str):
            match = pattern.fullmatch(obj)
            if match:
                key = match.group(1)
                if key not in config: raise ValueError(f"Unrecognized placeholder {key}")
                return config[key]
            return pattern.sub(lambda m: str(config[m.group(1)]), obj)
        return obj
    rendered = visit(template)
    if pattern.search(json.dumps(rendered)):
        raise ValueError("Unresolved placeholders remain")
    return rendered


def render_permissions(template, database_user):
    # Database identifiers are quoted in the template; reject quotes and SQL syntax.
    if not isinstance(database_user, str) or not re.fullmatch(r"[A-Za-z0-9_:+=,.@/-]{1,128}", database_user) or "REPLACE_WITH" in database_user:
        raise ValueError("Supply the actual workflow database identity (for example IAMR:<role-name>)")
    return template.replace("{{WORKFLOW_DB_USER}}", database_user)


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="action", required=True)
    e = sub.add_parser("extract", help="Read original deployed ASL and extract local config")
    e.add_argument("definition", type=Path)
    e.add_argument("--out", type=Path, default=Path("stepfunctions/config.local.json"))
    r = sub.add_parser("render", help="Render Git-safe template to a deployment JSON")
    r.add_argument("--template", type=Path, default=Path("stepfunctions/tickit_pipeline_v2.template.json"))
    r.add_argument("--config", type=Path, default=Path("stepfunctions/config.local.json"))
    r.add_argument("--out", type=Path, default=Path("stepfunctions/tickit_pipeline_v2.rendered.json"))
    permissions = sub.add_parser("render-permissions", help="Render SQL grants for a configured workflow database user")
    permissions.add_argument("--db-user", default=os.environ.get("TICKIT_WORKFLOW_DB_USER"))
    permissions.add_argument("--template", type=Path, default=Path("sql/08_workflow_permissions.sql"))
    permissions.add_argument("--out", type=Path, default=Path("sql/08_workflow_permissions.rendered.sql"))
    args = p.parse_args()
    if args.action == "extract":
        definition = json.loads(args.definition.read_text())
        config = validate_config(extract(definition))
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(config, indent=2) + "\n")
        args.out.chmod(0o600)
        print(f"Saved private config to {args.out} (excluded from git)")
    elif args.action == "render-permissions":
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(render_permissions(args.template.read_text(), args.db_user))
        print(f"Rendered {args.out}; apply after deploying the stored procedures")
    else:
        cfg = json.loads(args.config.read_text())
        tpl = json.loads(args.template.read_text())
        obj = render(tpl, cfg)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(obj, indent=2) + "\n")
        print(f"Rendered {args.out}; validate with AWS before deploying")

if __name__ == "__main__":
    main()
