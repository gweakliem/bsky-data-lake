# IAM bootstrap (admin, one time)

The dev SSO role (`PowerUserAccess`) can't create IAM roles or policies. Every EKS workload, the cluster itself and GitHub Actions need roles, so an IAM admin sets up **delegated, boundary-scoped IAM access** once. After that, the Pulumi stacks create and change all project roles themselves. This is decision D0 in [`docs/plans/deployment.md`](../../docs/plans/deployment.md).

These files are not a Pulumi project. They are applied by hand by someone with IAM and IAM Identity Center admin rights.

| File | What it is |
|---|---|
| `boundary-policy.json` | Permissions boundary `bsky-workload-boundary`. Every project role must carry it |
| `developer-inline-policy.json` | Inline policy for the developer's permission set: manage roles/policies under `/bsky/`, only with the boundary |

## How it works

1. **The boundary caps what any project role can do.** A role's effective permissions are the intersection of its own policies and its boundary.
   - The boundary allows everything except IAM, Organizations, account settings and Identity Center. It adds back the few IAM actions EKS Auto Mode roles need, plus `iam:PassRole` for `/bsky/` roles only.
   - It mirrors what the developer can already do with `PowerUserAccess`, so no role they create can exceed their own access.
   - Least privilege for each workload comes from the role's own policy, defined in Pulumi, not from the boundary.
2. **The developer can only create or change roles under `/bsky/`, and only with that boundary attached.**
   - This is enforced by the `iam:PermissionsBoundary` condition on `CreateRole`, `AttachRolePolicy` and `PutRolePolicy`.
   - Policies can only be created under `/bsky/`.
3. **The boundary itself is out of the developer's reach.** It lives at the root path, outside `/bsky/`, and an explicit Deny blocks removing a role's boundary or editing or deleting the boundary policy.
4. **The GitHub Actions OIDC provider is allowed by its exact ARN**, since OIDC provider ARNs can't use paths.

Without the boundary, the developer could create a role with `AdministratorAccess` and assume it.

## Admin steps

Use an admin profile for the account that holds the data lake. `us-west-2` is the region throughout.

### 1. Fill in the account ID

The templates use `<ACCOUNT_ID>` so no account ID is committed. Render them to a scratch directory outside the repo:

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --profile <admin-profile>)
OUT=$(mktemp -d)
for f in boundary-policy.json developer-inline-policy.json; do
  sed "s/<ACCOUNT_ID>/$ACCOUNT_ID/g" "$f" > "$OUT/$f"
done
echo "$OUT"
```

### 2. Create the boundary policy

```bash
aws iam create-policy --profile <admin-profile> \
  --policy-name bsky-workload-boundary \
  --description "Permissions boundary for bsky-data-lake roles (see infra/iam-bootstrap)" \
  --policy-document "file://$OUT/boundary-policy.json"
```

Leave it at the root path. The name `bsky-workload-boundary` is referenced by the inline policy and by the Pulumi stacks.

### 3. Create a permission set for the developer

In IAM Identity Center, in the organization's management account or the delegated Identity Center admin account:

1. **Permission sets → Create permission set → Custom permission set.**
2. Name it `BskyDataLakeDeveloper`, with a session duration of 8–12 hours.
3. Attach the AWS managed policy **`PowerUserAccess`**.
4. Add an **inline policy** with the contents of `$OUT/developer-inline-policy.json`.
5. **AWS accounts →** select the data-lake account **→ Assign users or groups →** choose the developer's user and this permission set.

Create a new permission set rather than editing the shared `PowerUserAccess` one. Every user assigned that one, in every account it's provisioned to, would get these IAM rights.

The CLI equivalent is `aws sso-admin create-permission-set`, `attach-managed-policy-to-permission-set`, `put-inline-policy-to-permission-set`, `create-account-assignment` and `provision-permission-set`. They need the Identity Center instance ARN from `aws sso-admin list-instances`.

### 4. Tell the developer it's done

They need the permission set name. Nothing else changes for them on the admin side.

## Developer steps (after the admin is done)

1. Add an SSO profile for the new permission set and use it from then on (shell, `cargo run` exports, and Pulumi):

   ```bash
   aws configure sso            # choose the BskyDataLakeDeveloper role; name the profile e.g. gweakliem-bsky
   export AWS_PROFILE=gweakliem-bsky
   aws sso login
   ```

2. In the Pulumi stacks:
   - every `aws.iam.Role` and `aws.iam.Policy` is created with `path="/bsky/"`;
   - every role is created with `permissions_boundary=f"arn:aws:iam::{account_id}:policy/bsky-workload-boundary"`, with the account ID taken from `aws.get_caller_identity()` at runtime;
   - `manageIam` is set to `true` in `Pulumi.dev.yaml`;
   - a Pulumi mock test fails if any role or policy is missing the path, or any role is missing the boundary.

## Verify

Run these with the developer profile:

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

# Should succeed: correct path and boundary.
aws iam create-role --role-name bsky-boundary-test --path /bsky/ \
  --assume-role-policy-document "$TRUST" \
  --permissions-boundary "arn:aws:iam::$ACCOUNT_ID:policy/bsky-workload-boundary"

# Should fail with AccessDenied: no boundary.
aws iam create-role --role-name bsky-boundary-test-2 --path /bsky/ --assume-role-policy-document "$TRUST"

# Should fail with AccessDenied: outside /bsky/.
aws iam create-role --role-name bsky-boundary-test-3 \
  --assume-role-policy-document "$TRUST" \
  --permissions-boundary "arn:aws:iam::$ACCOUNT_ID:policy/bsky-workload-boundary"

# Should fail with AccessDenied: removing the boundary.
aws iam delete-role-permissions-boundary --role-name bsky-boundary-test

# Clean up.
aws iam delete-role --role-name bsky-boundary-test
```

## Maintenance

- **Changing either policy is an admin task.** Edit the template here first, so the repo stays the record, then apply it:
  - boundary: `aws iam create-policy-version --set-as-default`. IAM keeps at most 5 versions, so delete old ones as needed;
  - inline policy: update the permission set and re-provision it.
- **Things that may need a change later:**
  - **EKS Auto Mode IAM actions.** The `IamNeededByEksRoles` list was written from memory of the AWS-managed `AmazonEKSClusterPolicy` and `AmazonEKSComputePolicy` and hasn't been verified yet. If the cluster can't launch nodes (D3), compare it with those managed policies' current IAM actions and add what's missing.
  - **IRSA.** If any workload has to use IRSA instead of Pod Identity, the developer policy needs the `*OpenIDConnectProvider` actions on the cluster's OIDC provider ARN, which is only known after the cluster exists.
  - **New AWS services** don't need boundary changes, because the boundary allows all non-IAM actions.
- **Revoking access:** remove the account assignment for the permission set. Roles already created keep working, since they don't depend on the developer's access.
