"""Deploy one CloudFormation stack through a change set.

    python scripts/deploy_stack.py <template> <stack-name> [Key=Value ...]

* Templates over CloudFormation's 51,200-byte inline limit are uploaded to S3 and deployed with --template-url
  (limit 1 MB). Set TEMPLATE_BUCKET (the artifacts bucket). Without it, a large template is sent with comments
  and blank lines removed, which only helps when it is just over the limit.
* Every stack gets the cost-allocation tags below. CloudFormation copies stack tags to every resource that
  supports tags, so all MediSlot resources carry Project / Environment / CostCenter (Milestone 7, AWS Budgets).
"""
import json
import os
import subprocess
import sys
import time
import uuid

template, stack_name, *parameter_pairs = sys.argv[1:]
region = os.environ.get("AWS_REGION", "ap-south-1")
environment = os.environ.get("ENVIRONMENT", "dev")
bucket = os.environ.get("TEMPLATE_BUCKET", "")
TEMPLATE_BODY_LIMIT = 51200
TAGS = {"Project": "MediSlot", "Environment": environment, "CostCenter": "medislot-capstone",
        "ManagedBy": "CloudFormation", "Stack": stack_name}


def aws(*args, check=True):
    return subprocess.run(["aws", *args, "--region", region], check=check, text=True, capture_output=True)


with open(template, encoding="utf-8") as fh:
    source = fh.read()
size = len(source.encode("utf-8"))
template_args = ["--template-body", f"file://{template}"]
if size > TEMPLATE_BODY_LIMIT:
    if bucket:
        key = f"cloudformation/{stack_name}/{uuid.uuid4().hex}.yaml"
        aws("s3", "cp", template, f"s3://{bucket}/{key}", "--sse", "AES256")
        template_args = ["--template-url", f"https://{bucket}.s3.{region}.amazonaws.com/{key}"]
        print(f"Template is {size} bytes (> {TEMPLATE_BODY_LIMIT}); deploying from s3://{bucket}/{key}.")
    else:
        lines = source.splitlines()
        if any(l.rstrip().endswith((": |", ": |-", ": >", ": >-", ": |+", ": >+")) for l in lines):
            raise SystemExit(f"{template} is over {TEMPLATE_BODY_LIMIT} bytes and has block scalars; set TEMPLATE_BUCKET.")
        compact = "\n".join(l for l in lines if l.strip() and not l.lstrip().startswith("#")) + "\n"
        if len(compact.encode("utf-8")) > TEMPLATE_BODY_LIMIT:
            raise SystemExit(f"{template} is too large even without comments; set TEMPLATE_BUCKET.")
        path = os.path.join(os.environ.get("RUNNER_TEMP", "/tmp"), "compact-" + os.path.basename(template))
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(compact)
        template_args = ["--template-body", f"file://{path}"]
        print(f"Template is over {TEMPLATE_BODY_LIMIT} bytes; sending a copy without comments.")

parameters = []
for pair in parameter_pairs:
    key, value = pair.split("=", 1)
    parameters.append(f"ParameterKey={key},ParameterValue={value}")

exists = aws("cloudformation", "describe-stacks", "--stack-name", stack_name, check=False).returncode == 0
kind = "UPDATE" if exists else "CREATE"
name = f"m7-{uuid.uuid4().hex[:12]}"
cmd = ["cloudformation", "create-change-set", "--stack-name", stack_name, *template_args,
       "--change-set-name", name, "--change-set-type", kind,
       "--capabilities", "CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND",
       "--tags", *[f"Key={k},Value={v}" for k, v in TAGS.items()]]
if parameters:
    cmd += ["--parameters", *parameters]
result = aws(*cmd, check=False)
if result.returncode:
    print(result.stderr)
    raise SystemExit(result.returncode)

for _ in range(90):
    info = aws("cloudformation", "describe-change-set", "--stack-name", stack_name,
               "--change-set-name", name, "--output", "json", check=False)
    if info.returncode == 0:
        data = json.loads(info.stdout)
        if data["Status"] in ("CREATE_COMPLETE", "FAILED"):
            break
    time.sleep(5)
else:
    raise SystemExit("Timed out waiting for Change Set creation")

print(json.dumps({"stack": stack_name, "type": kind, "status": data["Status"], "reason": data.get("StatusReason"),
                  "changes": [{"action": c["ResourceChange"]["Action"],
                               "logicalId": c["ResourceChange"]["LogicalResourceId"],
                               "type": c["ResourceChange"]["ResourceType"],
                               "replacement": c["ResourceChange"].get("Replacement")}
                              for c in data.get("Changes", [])]}, indent=2))
if data["Status"] != "CREATE_COMPLETE":
    reason = data.get("StatusReason", "")
    if "didn't contain changes" in reason or "No updates are to be performed" in reason:
        aws("cloudformation", "delete-change-set", "--stack-name", stack_name, "--change-set-name", name, check=False)
        print("No changes to deploy.")
        raise SystemExit(0)
    raise SystemExit("Change Set failed; see reason above.")

aws("cloudformation", "execute-change-set", "--stack-name", stack_name, "--change-set-name", name)
waiter = "stack-create-complete" if kind == "CREATE" else "stack-update-complete"
waited = aws("cloudformation", "wait", waiter, "--stack-name", stack_name, check=False)
if waited.returncode:
    events = aws("cloudformation", "describe-stack-events", "--stack-name", stack_name, "--max-items", "25",
                 "--query", "StackEvents[?contains(ResourceStatus, 'FAILED')].[LogicalResourceId,ResourceStatusReason]",
                 "--output", "text", check=False)
    print(events.stdout)
    raise SystemExit(f"Stack {stack_name} did not reach {waiter}; failed resources above.")
print(f"Stack {stack_name} deployed successfully.")
