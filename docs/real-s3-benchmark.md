# Real S3 benchmark

The `Real S3 benchmark` Actions workflow is a manually triggered,
`main`-only measurement. GitHub exchanges its OIDC token for a narrowly scoped
launcher role, starts one managed CodeBuild container in the bucket's AWS
region, waits for it, and preserves the output as a GitHub artifact. A managed
ARM CodeBuild container is the benchmark worker; no persistent self-hosted
runner or long-lived AWS key is involved.

Pull requests continue to run `cargo xtask s3` against pinned MinIO. The AWS
run uses the same ignored `object_storage` suite with CodeBuild's temporary
service-role credentials and a unique prefix. Benchmark data expires after
seven days; GitHub keeps the downloaded result for 30 days.

## One-time AWS setup

The account must have GitHub's OIDC provider registered with URL
`https://token.actions.githubusercontent.com` and audience `sts.amazonaws.com`.
Create it once per AWS account if it does not already exist:

```sh
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com
```

Pass that provider's ARN to the repository template:

```sh
aws cloudformation deploy \
  --stack-name moraine-real-s3-benchmark \
  --template-file .github/aws/real-s3-benchmark.yml \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides \
    GitHubOidcProviderArn=arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com
```

The template creates the private lifecycle-managed bucket, the ephemeral
CodeBuild project, its S3-only execution role, and an OIDC launcher role whose
trust policy accepts only `morainedb/moraine`'s `main` ref. The subject includes
GitHub's immutable organization and repository IDs, so a future rename,
transfer, or reuse of either name cannot inherit the trust relationship.

The parameter defaults target this repository:

| Parameter | Default |
|---|---|
| `GitHubOrganization` | `morainedb` |
| `GitHubOrganizationId` | `310371634` |
| `GitHubRepository` | `moraine` |
| `GitHubRepositoryId` | `1294736153` |

Override all four for a fork. Obtain the IDs with `gh api users/OWNER --jq .id`
and `gh api repos/OWNER/REPOSITORY --jq .id`.

Copy the four stack outputs into repository Actions variables:

| Repository variable | CloudFormation output |
|---|---|
| `MORAINE_BENCHMARK_AWS_REGION` | `AwsRegion` |
| `MORAINE_BENCHMARK_CODEBUILD_PROJECT` | `CodeBuildProjectName` |
| `MORAINE_BENCHMARK_S3_BUCKET` | `BenchmarkBucketName` |
| `MORAINE_BENCHMARK_ROLE_ARN` | `GitHubLauncherRoleArn` |

For example, fetch the outputs with:

```sh
aws cloudformation describe-stacks \
  --stack-name moraine-real-s3-benchmark \
  --query 'Stacks[0].Outputs' --output table
```

## Running it

Open **Actions → Real S3 benchmark → Run workflow** and select `main`. A
dispatch from any other ref fails before requesting AWS credentials, and the
AWS trust policy independently refuses its OIDC subject.

The Actions summary links to the CodeBuild logs and names the unique S3 data
prefix. The `real-s3-<run>-<attempt>` artifact contains the benchmark output and
CodeBuild metadata. CloudFormation retains the bucket when the stack is
deleted so benchmark data is not destroyed implicitly; empty it explicitly
when retiring the stack.
