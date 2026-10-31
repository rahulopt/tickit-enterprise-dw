# Separate AWS test account and dedicated CLI profile

**Deployment remains BLOCKED.** This is a guide for the account owner to follow manually. No account, identity, budget, CLI profile or AWS resource was created while preparing it. Do not create resources, execute SQL or invoke workloads in existing account **<EXISTING_AWS_ACCOUNT_ID>**. A new account/profile does not authorize deployment: provide the separate account ID, then explicitly authorize any later deployment.

## 1. Create a genuinely separate account

1. Open a separate browser profile/private session so the existing TICKIT account's console session cannot be selected accidentally.
2. Use the [AWS sign-up process](https://docs.aws.amazon.com/accounts/latest/reference/getting-started.html) to create a **standalone** account, with a unique email address you control and an identifiable name such as `tickit-isolated-test`. Account creation outside Organizations uses the web signup flow, not AWS CLI.
3. Complete email, contact, payment and identity verification. Review payment verification/plan terms. Select **Basic Support**; do not select a paid support upgrade or buy Marketplace products. Check the chosen account plan's Glue/Redshift access before accepting it; do not assume credits/free-tier eligibility cover this test.
4. Wait for account activation. In the account menu, record the **new 12-digit account ID**. It must differ from `<EXISTING_AWS_ACCOUNT_ID>`.
5. Enable root MFA, protect recovery details and do not create root access keys. Use root only for initial account/security/billing tasks; log out afterward. [Root MFA](https://docs.aws.amazon.com/IAM/latest/UserGuide/enable-mfa-for-root.html).
6. Do not create a bucket, VPC, Redshift workgroup, Glue job, pipeline, workload role or other deployment resource yet. Do not copy data, trust policies, keys or resource configuration from production/development.

If an administrator already operates a suitable **non-production organization**, they may create a dedicated member account instead. That administrator needs `organizations:CreateAccount` and account-status/read permissions in its management account, not in the new member account. Do not create/modify an organization or account through `<EXISTING_AWS_ACCOUNT_ID>` for this task. An Organizations-created account shares consolidated billing; budgets must filter the exact new linked account. [Account creation permissions](https://docs.aws.amazon.com/accounts/latest/reference/API_CreateAccount.html).

## 2. Establish minimum access in the NEW account

Keep bootstrap/account administration separate from the everyday CLI verification identity.

| Identity/task | Minimum scope for this stage |
|---|---|
| Account owner/bootstrap administrator | New-account signup, root MFA/recovery, creating the verification identity and billing notifications. No project deployment is needed. Use these powers manually, only in the new account. |
| Standalone CLI verifier | A non-root console IAM identity with MFA and `SignInLocalDevelopmentAccess` for temporary CLI login. Add narrowly scoped self-password/MFA-enrollment rights only if the user must manage those itself. No access keys, AdministratorAccess, PowerUserAccess, AssumeRole, PassRole or workload permissions. |
| Identity verification | `sts:GetCallerIdentity` needs **no IAM allow permission**. There is no reason to grant service read/write access merely to verify the account. |
| Billing operator | Separate account-owner/billing access to create/view the notification-only budget; verifier need not have billing access. In Organizations, use an authorized payer billing administrator. |
| Future deployment operator | **Not granted now.** Must be reviewed against the final account/prefix/templates after explicit deployment authorization. |

For the standalone route, use the new-account bootstrap administrator to create a console-only verification user, for example `tickit-test-verifier`, complete its password/MFA setup, and attach only the login policy plus any necessary self-service enrollment permissions. Do not attach a broad read-only policy: account identity verification does not require listing project resources.

AWS's login managed policy grants `signin:AuthorizeOAuth2Access` and `signin:CreateOAuth2Token`; it does not grant Glue/S3/Redshift deployment rights. [Policy reference](https://docs.aws.amazon.com/aws-managed-policy/latest/reference/SignInLocalDevelopmentAccess.html), [STS permission requirement](https://docs.aws.amazon.com/STS/latest/APIReference/API_GetCallerIdentity.html).

If using an existing Organizations IAM Identity Center setup, have its administrator assign a dedicated verification permission set to **only the new account**. An Identity Center **account instance** cannot provide account permission sets/CLI account access; that requires an organization instance. Do not enable Organizations in the existing project account just to obtain SSO. [Instance differences](https://docs.aws.amazon.com/singlesignon/latest/userguide/identity-center-instances.html).

## 3. Configure a NEW named AWS CLI profile

Commands below are for you to run after the new account and verification identity exist. They configure/authenticate only; none deploy resources. Do not overwrite `default`, change its keys, or use production credentials as a `source_profile`.

Check the installed CLI locally:

```sh
aws --version
```

**Standalone account:** use AWS CLI **2.32.0 or newer**, which supports temporary browser-based login:

```sh
aws configure set region us-east-1 --profile tickit-test-verify
aws configure set output json --profile tickit-test-verify
aws login --profile tickit-test-verify
```

In the browser, select/sign in as the **non-root verifier in the new account**, complete MFA and confirm the account ID before approving login. Do not select an existing production/development console session. This login uses temporary credentials; no `aws configure` access-key entry is needed. [AWS CLI browser login](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sign-in.html).

**Existing organization SSO alternative:** instead of `aws login`, use:

```sh
aws configure sso --profile tickit-test-verify
aws sso login --profile tickit-test-verify
```

Supply your administrator's access portal/start URL, SSO region and the **new account's verification role**. Set the CLI default service region to `us-east-1`, output JSON, profile `tickit-test-verify`. The SSO region can differ from the service region. [SSO profile setup](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html).

Always supply `--profile tickit-test-verify --region us-east-1` explicitly. Do not globally switch `AWS_PROFILE` for existing tools. Keep config/credential/login caches outside Git; never paste keys, session tokens, OAuth URLs/codes or MFA recovery details into this repository/chat. Temporary CLI login compatibility with any future SDK-based deployment tooling must be checked separately before that tooling is authorized.

## 4. Verify STS identity and reject the existing account

Run this read-only command with static credential environment overrides cleared **for this subprocess only**:

```sh
env -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN -u AWS_SECURITY_TOKEN aws sts get-caller-identity --profile tickit-test-verify --region us-east-1 --output json --no-cli-pager
```

Check that `Account` is the new account ID and `Arn` identifies the intended non-root user/role. To enforce those checks, replace the placeholder below with your new account ID and run this verification-only snippet:

```sh
python3 - REPLACE_WITH_NEW_TEST_ACCOUNT_ID <<'PY'
import json
import os
import re
import subprocess
import sys

expected = sys.argv[1]
if not re.fullmatch(r'[0-9]{12}', expected) or expected == '<EXISTING_AWS_ACCOUNT_ID>':
    raise SystemExit('STOP: supply a distinct, valid test account ID')
verification_env = os.environ.copy()
for name in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
             'AWS_SESSION_TOKEN', 'AWS_SECURITY_TOKEN'):
    verification_env.pop(name, None)
result = subprocess.run(
    ['aws', 'sts', 'get-caller-identity', '--profile', 'tickit-test-verify',
     '--region', 'us-east-1', '--output', 'json', '--no-cli-pager'],
    env=verification_env, check=True, capture_output=True, text=True)
identity = json.loads(result.stdout)
if identity['Account'] != expected or identity['Arn'].endswith(':root'):
    raise SystemExit('STOP: unexpected account or root identity; deployment stays blocked')
print(json.dumps({'account': identity['Account'], 'arn': identity['Arn'],
                  'profile': 'tickit-test-verify', 'service_region': 'us-east-1',
                  'identity_verified': True, 'deployment_authorized': False}, indent=2))
PY
```

STS validates identity, **not** deployment permissions, resource-name uniqueness, regional quotas, or cost safeguards. A successful result is not authorization to create anything. If credentials expire, repeat the corresponding login command and verify STS again; never fall back to `default` or add access keys to bypass a login problem.

## 5. Put cost safeguards in place before any deployment request

Using the new account's billing console (or authorized payer administrator for a member account):

1. Create a **notification-only $5 USD monthly cost budget** for the exact new account. Prefer an all-services/unblended view without masking charges with credits; understand tax/support inclusion. Verify its account scope and recipient email.
2. Add actual-spend notifications at **$1, $3 and $5**, and a forecast notification at $5 where forecast data is available. Keep the budget through final billing review after cleanup. A new account may lack enough history for a useful forecast.
3. Use email notification only for setup; no SNS/Lambda budget automation, paid monitoring extension or budget action is required for this preparation. Review any budget pricing/plan terms before saving it. No budget was created by this agent.
4. Treat $3 observed/projected test spend as the operational stop margin. Use service runtimes as well as billing data, which can lag. Budget alerts **do not stop resources or guarantee a $5 maximum**. [AWS Budgets delays](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-managing-costs.html).
5. Keep these **future workload controls** in the deployment review: four Glue jobs at two G.1X workers, five-minute limits, zero retries; Redshift eight base/max RPUs and a four-RPU-hour/day deactivation usage limit; 120-second SQL limits; 900-second workflow timeout; one initial admission plus one duplicate-delivery check. **Do not create those resources or controls now.**
6. Reconfirm prices/service eligibility and the intended total budget before authorizing deployment. Existing project estimate: ordinary test $0.40–$1.00; intended total $5, with cancellation/cleanup reserve. There is no guaranteed cap; delayed shutdown, enforcement and retained storage/secrets can exceed estimates. Do not launch paid workloads to test Free Tier eligibility.

## 6. Review future deployment permissions without granting them

The verification profile should remain unable to deploy. Once the new ID/prefix are known, review a separate short-lived deployment identity with only the capabilities needed by the three templates:

- CloudFormation operations on exact new test stacks and inspection of their events/outputs; a reviewed test-only execution role if that deployment model is chosen.
- Artifact uploads only to the new test bucket's `artifacts/` keys, with conditional PUTs.
- Creation/configuration/cleanup of only the named test IAM roles/policies, S3 bucket/policy, DynamoDB ledger, SQS/SNS resources, Lambda layers/functions, Glue jobs, Standard state machine, disabled EventBridge rule and logs.
- Dedicated test VPC/subnets/security group and Redshift namespace/workgroup, including required service-linked-role dependencies. Some create/read APIs require wildcard resources; scope them with supported account/region/request-tag conditions and review unavoidable exceptions rather than claiming name-only restrictions suffice.
- `iam:PassRole` only for exact reviewed execution roles and intended AWS services. No unrestricted PassRole or cross-account AssumeRole.
- Bootstrap SQL/admin-secret access, workload execution and cleanup permissions only when explicitly authorized. Provisioning permission is not permission to start jobs or execute SQL.

This is a permission **review checklist**, not an executable minimum deployment policy. Exact permissions depend on rendered resource ARNs and CloudFormation execution model and must be finalized after account verification. Do not attach AdministratorAccess/PowerUserAccess to the verifier to make a future deployment work. The project already contains scoped runtime service policies in `infra/isolated/`; those policies are not a deployment-operator policy.

## 7. Supply verification evidence, then stop

Provide:

- New 12-digit test account ID, account type (standalone/member), CLI profile `tickit-test-verify` and verified non-root STS ARN. No credentials or authentication codes.
- Service region `us-east-1` and the intended test prefix. Current proposal is `tickit-it-20261031-0d1102`; syntax is validated, cloud uniqueness remains pending.
- Confirmation that root/operator MFA and the $5 budget notifications are configured in the **new account**; note any permissions/service-plan restrictions.
- Any later explicit authorization, distinguishing **provisioning only** from permission to execute bootstrap SQL, run the single workflow, inspect results and clean up.

I will keep deployment blocked until the separate account ID is supplied, target checks are complete and you explicitly authorize deployment. Do not run the existing deployment runbook merely because STS succeeded.

## 8. Understand cleanup before authorizing charges

After any separately approved test: stop admissions, prove child Glue/SQL work has stopped, save evidence, then remove runtime, usage limit, warehouse and foundation in dependency order. Retained S3 bucket/ledger require separate cleanup; remove every object version/delete marker, residual backups, layer versions, logs, managed secret/snapshots and test networking as applicable. Retain billing notifications until residual charges are checked. Do not close the AWS account as a substitute for deleting workloads or accounting for charges. Full steps: [integration cleanup](AWS_INTEGRATION_RUNBOOK.md#failure-rollback-and-cleanup).

Current state and evidence: [deployment preflight](DEPLOYMENT_PREFLIGHT.md). No AWS account setup, profile configuration or AWS API call was performed while writing this guide.
