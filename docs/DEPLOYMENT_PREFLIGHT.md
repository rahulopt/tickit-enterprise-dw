# Final deployment preflight — BLOCKED

Separate-account creation and dedicated-profile instructions are in [SEPARATE_TEST_ACCOUNT_SETUP.md](SEPARATE_TEST_ACCOUNT_SETUP.md). These are manual instructions only; deployment remains blocked.

Checked 2026-10-31. Authorization covers preparation and read-only verification only. **No deployment, billable resource creation, Lambda invocation, Glue run, Redshift SQL or workflow execution is authorized.** Conditional approval is not deployment authorization. Existing production and development resources must remain untouched.

## Target verification

| Prerequisite | Result | Evidence / remaining action |
|---|---|---|
| Current AWS identity | PASS — identified, unsuitable for test | Successful `aws sts get-caller-identity --region us-east-1`; account matches existing TICKIT resource ARNs in private local configuration. Exact identity evidence is in the ignored preflight record. |
| Separate test account/profile | **FAIL — not configured/verified** | Only `default` is configured; isolated configuration contains placeholders. Do not reuse the existing project account or create an account automatically. User must identify an existing separate test account/profile before target verification can finish. |
| Region | PASS locally / PENDING target | Default profile is configured for `us-east-1`; all three templates restrict that region. No separate test profile/quotas are verified. |
| Proposed unique prefix | PASS syntax / PENDING collision check | `tickit-it-20261031-0d1102`. Valid under all templates' naming rules. Random suffix is not proof of cloud uniqueness; check names in the identified test account before authorization. No names have been reserved. |
| Account/region guards | PASS offline | Every resource has `IsolatedAccount`; condition excludes the supplied production account and other regions. With the observed existing-project account as the excluded account, all three stacks would be disabled. Never supply a false production ID to bypass this guard. |
| Actual IAM/resource isolation | PENDING | No test deployment exists. Definitions scope roles to the test bucket/jobs/workflow/workgroup; runtime references use same-prefix test-stack exports. Actual service policies, identities and absence of existing-resource collisions need target read-only review. |
| Billing safeguards | PASS design / PENDING enforcement | No controls have been installed. Their future verification is mandatory before any compute/SQL activation. |
| Cleanup | PASS documented / PENDING target inventory | Reverse-dependency cleanup and retained-resource handling reviewed below. There is currently no AWS test deployment to clean up. |
| Deployment authorization | **PENDING — explicitly withheld** | A further explicit deployment authorization is required after the target checks are resolved and reviewed. |

Private evidence: `tests/isolated/generated/target-preflight.local.json` (ignored by Git, mode 0600). It records current account/profile, comparison with existing configuration, region, proposed prefix, guard evaluations and the non-authorization state. No credentials are stored. The only successful AWS API request during this preflight was STS GetCallerIdentity. No Cost Explorer, S3, DynamoDB or other potentially metered service requests were made.

## Isolation boundaries to preserve

- Use a separate existing AWS account; all new names must use the approved test prefix. Do not modify any existing production or development role, bucket notification, queue, schedule, workgroup, database, schema or table.
- New test namespace/workgroup/database, versioned encrypted S3 bucket, ledger, queues/topic, jobs/functions and Standard workflow only. No production snapshots, shares, source data, network peering or cross-account role assumption.
- Ingress cannot start workflows. Workers cannot execute SQL or submit pipelines. Controller invokes only named test jobs/functions and executes SQL only on the new test workgroup. Status-only DescribeStatement uses wildcard resource because of API permission limitations; separate-account separation remains essential.
- Conditional create-only writes/no-delete policy protect source, final output, warehouse and artifact keys. Spark scratch is deliberately scoped separately; it is not a COPY input. Verify actual policies after authorized provisioning; definitions alone do not prove enforcement.
- All schedules remain disabled and notifications absent until a separately authorized activation. Supplying a valid prefix or successfully creating resources must not implicitly authorize an execution.

## Cost verification and safeguards

The intended total test budget is **$5**, including bootstrap, one initial admission, one identical-delivery check, validation and cleanup. It excludes unrelated existing-account usage and taxes. **It is not a guaranteed billing cap.** No billable test resources or workloads were created during preparation.

Rechecked official pricing pages: Glue's example rate is $0.44/DPU-hour; Redshift's US East (N. Virginia) example uses $0.375/RPU-hour. These support the planning arithmetic but are not an account-specific quote. Confirm applicable regional/account rates immediately before authorization. [Glue pricing](https://aws.amazon.com/glue/pricing/), [Redshift pricing](https://aws.amazon.com/redshift/pricing/).

| Item | Planning calculation / safeguard |
|---|---|
| Four Glue jobs | Four × two G.1X DPUs × five minutes × $0.44/hour = approximately **$0.2934** if each uses the full proposed runtime. Zero retries; one concurrent run per job; queuing disabled. Timeout enforcement/billing can lag. |
| Redshift | Eight base/max RPUs; proposed four RPU-hours/day deactivation threshold corresponds to **$1.50** compute at the example rate, before enforcement lag. This is a usage threshold, not a dollar cap. |
| Other services | Reserve **$0.25–$0.50** for Lambda, state transitions, S3 requests/storage, SQS/SNS, ledger/PITR, logs and managed secret; this is an allowance, not measured consumption. |
| Expected ordinary total | Retain **$0.40–$1.00** as an illustrative estimate for a prompt small test; setup delays, errors or retained resources can exceed it. |
| Intentional stop margin | At **$3 observed or projected** attributable spend, stop adding work and begin authorized cancellation/cleanup, reserving intended budget for enforcement lag. Billing data is delayed: also track job/statement runtime and wall-clock duration, not billing alone. |

Before execution authorization, require the future test workgroup's usage-limit request to be reviewed; after any separately authorized provisioning, read back the installed limit and capacity/job settings before authorizing compute. The prepared request uses `amount=4`, `period=daily`, `usageType=serverless-compute`, `breachAction=deactivate`. [AWS usage-limit API](https://docs.aws.amazon.com/redshift-serverless/latest/APIReference/API_CreateUsageLimit.html).

The proposed controls are five-minute Glue limits, 900-second workflow timeout, 120-second SQL/user limits, 8-RPU max capacity, zero submission retries and one initial batch only. No NAT, crawler or production-sized data copy is proposed. A workflow timeout does not stop child jobs automatically; the registry/draining checks and exact-ID cancellation are required. Daily limits reset and cannot enforce a whole-test dollar maximum. Retained storage/PITR/secrets can continue charging after compute stops.

No budget alarm, usage limit or other billing resource was installed in this preflight. No paid billing API was used to try to verify a nonexistent test environment.

## Cleanup and rollback verification

1. Stop new test admissions: disable only the new test notification and watchdog. Preserve batch/queue/lease evidence.
2. Stop only the test execution if active. Cancel exact registered Glue runs/SQL statements and poll terminal states. Unknown submissions block release; do not clear leases or infer rollback from missing receipts.
3. Export test receipts/run evidence. A committed receipt represents committed data even when SNS/workflow completion fails. Keep production and development data untouched; dispose of the isolated test database after quiescence instead of manually undoing SCD2 history.
4. Delete runtime stack first, then the separately created usage limit and warehouse stack, then foundation. Save stack inventory to catch partial failures and retained resources. Delete managed-secret/snapshot remnants only if they belong to this test and cleanup is authorized.
5. Foundation bucket and ledger are **Retain + UpdateReplacePolicy Retain**. Deleting the stack is insufficient. After workers stop and cleanup is authorized, remove only the test immutable policy, delete all test object versions/delete markers/incomplete uploads, then delete the retained bucket/ledger and any retained backups.
6. Inspect remaining test-only queues, topic/subscription, layer versions, log groups, managed secret/snapshots, networking and usage limit. Confirm no billable test remnants remain; a one-day log retention policy does not replace cleanup. Never delete resources based only on a broad `tickit` name match.

The full implementation/deployment sequence and native test queries remain in [AWS_INTEGRATION_RUNBOOK.md](AWS_INTEGRATION_RUNBOOK.md). That runbook describes proposed future actions, not currently authorized commands.

## Final gate

**Do not deploy.** Obtain the separate test account ID and profile, verify identity and prefix non-collision using read-only checks, resolve actual isolation/quotas, and present the resolved target summary. Then wait for explicit deployment authorization. A later deployment authorization must state whether it permits provisioning only or also bootstrap SQL/workflow execution and cleanup; never infer execution permission from permission to prepare or provision.
