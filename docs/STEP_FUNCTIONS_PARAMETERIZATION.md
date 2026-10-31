# TICKIT V2 Step Functions — parameterized configuration

The template configures the six ETL branches and one atomic warehouse call (staging → daily SCD2 dimensions → temporal facts), followed by SNS. Resource identifiers and wait times are rendered from local configuration.

**Important:** Resource configuration is supplied at deployment time; every execution must also supply a runtime `load_date` string (YYYY-MM-DD). Rendering creates a real Step Functions definition containing the target resource identifiers; do not commit the rendered output or the local config. Existing executions are unaffected until `update-state-machine` is called. **No execution is started by these commands.**

## From inside the repository

`stepfunctions/tickit_pipeline_v2.template.json` is the single workflow source. The renderer and tests both use this file. `config.example.json` supplies configuration fields, not a second workflow; rendering produces a deployment artifact that is excluded from Git.

Set `STATE_MACHINE_ARN` and `AWS_DEFAULT_REGION` in your local environment first (see `.env.example`).

1. Back up your current state-machine definition and discover configuration from the running AWS resource:

```bash
aws stepfunctions describe-state-machine --state-machine-arn "$STATE_MACHINE_ARN" --region "$AWS_DEFAULT_REGION" --query definition --output text > /tmp/tickit-v2-live-before.json
python3 tools/tickit_workflow.py extract /tmp/tickit-v2-live-before.json
```

2. Render a standalone ASL definition and validate it **without running any ETL**:

```bash
python3 tools/tickit_workflow.py render
aws stepfunctions validate-state-machine-definition --definition file://stepfunctions/tickit_pipeline_v2.rendered.json --type STANDARD --severity WARNING --region "$AWS_DEFAULT_REGION" --output json
```

3. Compare current and rendered definitions. Review the new input validation and the replacement of three separate warehouse statements with `sp_load_warehouse`. Deploy the SQL procedures first; see [SCD2 upgrade instructions](SCD2.md):

```bash
python3 -m json.tool /tmp/tickit-v2-live-before.json > /tmp/tickit-v2-live-pretty.json
python3 -m json.tool stepfunctions/tickit_pipeline_v2.rendered.json > /tmp/tickit-v2-new-pretty.json
diff -u /tmp/tickit-v2-live-pretty.json /tmp/tickit-v2-new-pretty.json
```

4. **Optional only after validation and diff review:** change the deployed state machine **without starting an execution**. This action needs permission to update the state machine:

```bash
aws stepfunctions update-state-machine --state-machine-arn "$STATE_MACHINE_ARN" --definition file://stepfunctions/tickit_pipeline_v2.rendered.json --region "$AWS_DEFAULT_REGION"
```

5. Commit only the template, example configuration, renderer, and documentation:

```bash
git add stepfunctions/tickit_pipeline_v2.template.json stepfunctions/config.example.json .gitignore tools/tickit_workflow.py docs/STEP_FUNCTIONS_PARAMETERIZATION.md
git diff --cached --stat
git commit -m "Parameterize Step Functions environment configuration"
git push origin main
```

### Configuration fields

`CATEGORY_LAMBDA_ARN`, `VENUE_LAMBDA_ARN`, `USERS_GLUE_JOB`, `EVENTS_GLUE_JOB`, `LISTINGS_GLUE_JOB`, `SALES_GLUE_JOB`, `REDSHIFT_WORKGROUP`, `REDSHIFT_DATABASE`, `S3_BUCKET`, `REDSHIFT_COPY_ROLE_ARN`, `SNS_TOPIC_ARN`, `WAIT_STAGING_SECONDS`, `WAIT_DIMENSIONS_SECONDS`, `WAIT_FACTS_SECONDS`, `WORKFLOW_TIMEOUT_SECONDS`.

The legacy `WAIT_DIMENSIONS_SECONDS` and `WAIT_FACTS_SECONDS` configuration fields remain accepted for extraction compatibility; only `WAIT_STAGING_SECONDS` controls polling of the combined warehouse statement.

### Further improvements / limitations

- Daily SCD2 deployment and a warehouse-only execution were verified on 31 October 2026; see the [deployment evidence](SCD2_DEPLOYMENT_2026-10-31.md) and [migration requirements](SCD2.md).
- An `OK` result from AWS definition validation is **not** a successful end-to-end test. The complete Step Functions V2 execution remains untested.
- Redshift `CALL` statements remain the stored-procedure interface (`sp_load_warehouse`, which calls the three individual loaders). Stored procedure names aren't environment-specific and are intentionally fixed.
- The original flow runs all six ETL branches on invocation; it is chargeable. Do not start an execution solely to verify this template.
- The workflow uses Redshift Data API polling and job names, so its IAM execution role must be scoped to resources in the selected account/region. Parameterizing a definition doesn't grant permissions.
- Securely manage local configuration (`config.local.json`) and don't commit rendered output. Real ARNs and bucket names are configuration, not credentials, but are still typically kept out of public source templates.
- A production future improvement is use CloudFormation/CDK or parameter store for automated environment config and carefully versioned deployment, with approval for definition updates.
