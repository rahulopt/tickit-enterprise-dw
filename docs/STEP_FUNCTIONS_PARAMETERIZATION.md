# TICKIT V2 Step Functions — parameterized configuration

This update preserves the deployed 6-branch ETL → Redshift staging → dimensions → facts → SNS flow, but removes resource-specific names/ARNs, S3 bucket, database, and wait times from the Git-tracked workflow definition.

**Important:** This is a deployment-time template, not a runtime JSONPath-input design. Rendering creates a real Step Functions definition containing the target resource identifiers; do not commit the rendered output or the local config. Existing executions are unaffected until `update-state-machine` is called. **No execution is started by these commands.**

## From inside the repository

`stepfunctions/tickit_pipeline_v2.template.json` is the single workflow source. The renderer and tests both use this file. `config.example.json` supplies configuration fields, not a second workflow; rendering produces a deployment artifact that is excluded from Git.

1. Back up your current state-machine definition and discover configuration from the running AWS resource:

```bash
aws stepfunctions describe-state-machine --state-machine-arn arn:aws:states:us-east-1:519749210589:stateMachine:tickit-nightly-pipeline-v2 --region us-east-1 --query definition --output text > /tmp/tickit-v2-live-before.json
python3 tools/tickit_workflow.py extract /tmp/tickit-v2-live-before.json
```

2. Render a standalone ASL definition and validate it **without running any ETL**:

```bash
python3 tools/tickit_workflow.py render
aws stepfunctions validate-state-machine-definition --definition file://stepfunctions/tickit_pipeline_v2.rendered.json --type STANDARD --severity WARNING --region us-east-1 --output json
```

3. Compare current and rendered definitions. The expected substantive differences are the comment and the corrected SNS success description, not resource routing:

```bash
python3 -m json.tool /tmp/tickit-v2-live-before.json > /tmp/tickit-v2-live-pretty.json
python3 -m json.tool stepfunctions/tickit_pipeline_v2.rendered.json > /tmp/tickit-v2-new-pretty.json
diff -u /tmp/tickit-v2-live-pretty.json /tmp/tickit-v2-new-pretty.json
```

4. **Optional only after validation and diff review:** change the deployed state machine **without starting an execution**. This action needs permission to update the state machine:

```bash
aws stepfunctions update-state-machine --state-machine-arn arn:aws:states:us-east-1:519749210589:stateMachine:tickit-nightly-pipeline-v2 --definition file://stepfunctions/tickit_pipeline_v2.rendered.json --region us-east-1
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

### Further improvements / limitations

- This does not add a complete SCD Type 2 implementation. The success notification is updated to describe only the **initial/idempotent dimension loader**.
- An `OK` result from AWS definition validation is **not** a successful end-to-end test. The complete Step Functions V2 execution remains untested.
- Redshift `CALL` statements remain the stored-procedure interface (`sp_load_staging`, `sp_load_dimensions`, `sp_load_facts`). Stored procedure names aren't environment-specific and are intentionally fixed.
- The original flow runs all six ETL branches on invocation; it is chargeable. Do not start an execution solely to verify this template.
- The workflow uses Redshift Data API polling and job names, so its IAM execution role must be scoped to resources in the selected account/region. Parameterizing a definition doesn't grant permissions.
- Securely manage local configuration (`config.local.json`) and don't commit rendered output. Real ARNs and bucket names are configuration, not credentials, but are still typically kept out of public source templates.
- A production future improvement is use CloudFormation/CDK or parameter store for automated environment config and carefully versioned deployment, with approval for definition updates.
