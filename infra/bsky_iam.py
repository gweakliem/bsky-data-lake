"""Creates the project's IAM roles and policies within the delegated scope.

The dev SSO role may only create roles and policies under IAM_PATH, and roles
only with the bsky-workload-boundary permissions boundary attached (see
iam-bootstrap/). Every Role and Policy in these stacks goes through here so
that can't be missed; test_infra.py checks it.
"""

import functools

import pulumi
import pulumi_aws as aws

IAM_PATH = "/bsky/"
BOUNDARY_POLICY_NAME = "bsky-workload-boundary"

_MANAGED = {"path", "permissions_boundary"}


@functools.cache
def boundary_arn() -> pulumi.Output[str]:
    account_id = aws.get_caller_identity_output().account_id
    return account_id.apply(lambda a: f"arn:aws:iam::{a}:policy/{BOUNDARY_POLICY_NAME}")


def role(resource_name: str, opts: pulumi.ResourceOptions | None = None, **kwargs) -> aws.iam.Role:
    """An IAM role under IAM_PATH with the workload permissions boundary."""
    if overridden := _MANAGED & kwargs.keys():
        raise ValueError(f"bsky_iam.role sets {sorted(overridden)} itself")
    return aws.iam.Role(resource_name, path=IAM_PATH, permissions_boundary=boundary_arn(), opts=opts, **kwargs)


def policy(resource_name: str, opts: pulumi.ResourceOptions | None = None, **kwargs) -> aws.iam.Policy:
    """A customer-managed IAM policy under IAM_PATH."""
    if "path" in kwargs:
        raise ValueError("bsky_iam.policy sets path itself")
    return aws.iam.Policy(resource_name, path=IAM_PATH, opts=opts, **kwargs)
