"""natMode=instance (fck-nat), checked in a subprocess.

test_platform.py loads the stack once with the default config (the managed NAT
gateway). Stack config is read when the module loads, so the instance path runs
in its own interpreter with PULUMI_CONFIG set, reusing test_platform's mocks.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent

SCRIPT = r'''
import base64, importlib.util, json, os, sys
from pathlib import Path
here = Path(sys.argv[1])
src = (here / "test_platform.py").read_text().split('os.environ["BSKY_EKS_PUBLIC_CIDRS"]')[0]
ns = {}
exec(src, ns)
os.environ["BSKY_EKS_PUBLIC_CIDRS"] = "203.0.113.7/32"
import pulumi
mocks = ns["Mocks"]()
pulumi.runtime.set_mocks(mocks, preview=False)
spec = importlib.util.spec_from_file_location("platform_stack", here / "__main__.py")
stack = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stack)

@pulumi.runtime.test
def collect():
    def dump(args):
        routes = args[0]
        r = mocks.resources
        (lt,) = r["aws:ec2/launchTemplate:LaunchTemplate"]
        print(json.dumps({
            "nat_gateways": len(r["aws:ec2/natGateway:NatGateway"]),
            "routes": routes,
            "enis": r["aws:ec2/networkInterface:NetworkInterface"],
            "eip_associations": r["aws:ec2/eipAssociation:EipAssociation"],
            "policies": r["aws:iam/rolePolicy:RolePolicy"],
            "asgs": r["aws:autoscaling/group:Group"],
            "template": {**lt, "userData": base64.b64decode(lt["userData"]).decode()},
            "roles": r["aws:iam/role:Role"],
            "profiles": r["aws:iam/instanceProfile:InstanceProfile"],
        }, default=str))
    # Wait for everything the checks read, not just the route.
    return pulumi.Output.all(stack.private_rt.routes, stack.nat_asg.id, stack.nat_template.id,
                             stack.nat_profile.id, stack.nat_eni_policy.id).apply(dump)

collect()
'''


def _run() -> dict:
    env = {**os.environ, "PULUMI_CONFIG": json.dumps({"project:natMode": "instance"})}
    out = subprocess.run([sys.executable, "-c", SCRIPT, str(HERE)], env=env, capture_output=True, text=True, check=True)
    return json.loads(out.stdout.strip().splitlines()[-1])


RESULT = _run()


def test_no_nat_gateway_and_the_private_route_targets_the_fixed_interface():
    assert RESULT["nat_gateways"] == 0
    (route,) = RESULT["routes"]
    assert route["cidr_block"] == "0.0.0.0/0"
    assert route["network_interface_id"] == "nat-id"  # the mock id of the NetworkInterface named "nat"
    (eni,) = RESULT["enis"]
    assert eni["sourceDestCheck"] is False
    # fck-nat moves the Elastic IP onto the instance's own interface at boot (eip_id),
    # so nothing associates it with the fixed interface.
    assert RESULT["eip_associations"] == []


def test_one_instance_replaced_on_failure_in_the_interfaces_zone():
    (asg,) = RESULT["asgs"]
    assert (asg["minSize"], asg["maxSize"], asg["desiredCapacity"]) == (1, 1, 1)
    assert asg["vpcZoneIdentifiers"] == ["public-0-id"]  # where the fixed interface lives
    t = RESULT["template"]
    assert t["instanceType"] == "t4g.nano"
    assert t["metadataOptions"]["httpTokens"] == "required"
    assert "eni_id=nat-id" in t["userData"]
    assert "eip_id=" in t["userData"]


def test_nat_role_can_fix_its_own_interface_and_take_the_eip():
    (p,) = RESULT["policies"]
    actions = {st["Action"] for st in json.loads(p["policy"])["Statement"]}
    assert {"ec2:AttachNetworkInterface", "ec2:ModifyNetworkInterfaceAttribute", "ec2:AssociateAddress"} <= actions


def test_nat_role_and_profile_in_delegated_scope():
    nat = [r for r in RESULT["roles"] if r["name"] == "bsky-nat"]
    assert nat and nat[0]["path"] == "/bsky/"
    assert nat[0]["permissionsBoundary"].endswith(":policy/bsky-workload-boundary")
    (profile,) = RESULT["profiles"]
    assert profile["path"] == "/bsky/"
