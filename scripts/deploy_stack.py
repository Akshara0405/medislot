import sys, subprocess, uuid, os, json, time

template, stack_name, *parameter_pairs = sys.argv[1:]
region = os.environ.get("AWS_REGION", "ap-south-1")

# CloudFormation rejects an inline --template-body over 51,200 bytes. Comments and blank lines count toward that,
# so a large template is sent with full-line comments and blank lines removed (the file in the repo is unchanged).
# Only done for templates without YAML block scalars (| or >), where a "#" line could be real content.
TEMPLATE_BODY_LIMIT = 51200
with open(template, encoding="utf-8") as fh:
    source = fh.read()
if len(source.encode("utf-8")) > TEMPLATE_BODY_LIMIT:
    lines = source.splitlines()
    has_block_scalar = any(l.rstrip().endswith((": |", ": |-", ": >", ": >-", ": |+", ": >+")) for l in lines)
    if has_block_scalar:
        raise SystemExit(f"{template} is over {TEMPLATE_BODY_LIMIT} bytes and has block scalars; "
                         "upload it to S3 and deploy with --template-url instead.")
    compact = "\n".join(l for l in lines if l.strip() and not l.lstrip().startswith("#")) + "\n"
    size = len(compact.encode("utf-8"))
    if size > TEMPLATE_BODY_LIMIT:
        raise SystemExit(f"{template} is {size} bytes even without comments (limit {TEMPLATE_BODY_LIMIT}); "
                         "split the stack or deploy it from S3 with --template-url.")
    template = os.path.join(os.environ.get("RUNNER_TEMP", "/tmp"), "compact-" + os.path.basename(template))
    with open(template, "w", encoding="utf-8") as fh:
        fh.write(compact)
    print(f"Template is over {TEMPLATE_BODY_LIMIT} bytes; sending a {size}-byte copy without comments.")
parameters = []
for pair in parameter_pairs:
    key, value = pair.split("=", 1)
    parameters.append({"ParameterKey": key, "ParameterValue": value})
def aws(*args, check=True):
    return subprocess.run(["aws", *args, "--region", region], check=check, text=True, capture_output=True)
exists = aws("cloudformation", "describe-stacks", "--stack-name", stack_name, check=False).returncode == 0
kind = "UPDATE" if exists else "CREATE"
name = f"m2-{uuid.uuid4().hex[:12]}"
cmd = ["cloudformation", "create-change-set", "--stack-name", stack_name,
       "--template-body", f"file://{template}", "--change-set-name", name,
       "--change-set-type", kind, "--capabilities", "CAPABILITY_NAMED_IAM",
       "--parameters"] + [f"ParameterKey={p['ParameterKey']},ParameterValue={p['ParameterValue']}" for p in parameters]
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
print(json.dumps({"stack": stack_name, "type": kind, "status": data["Status"],
                  "reason": data.get("StatusReason"), "changes": [
                      {"action": c["ResourceChange"]["Action"],
                       "logicalId": c["ResourceChange"]["LogicalResourceId"],
                       "type": c["ResourceChange"]["ResourceType"]}
                      for c in data.get("Changes", [])]}, indent=2))
if data["Status"] != "CREATE_COMPLETE":
    reason = data.get("StatusReason", "")
    if "didn't contain changes" in reason or "No updates are to be performed" in reason:
        print("No changes to deploy.")
        raise SystemExit(0)
    raise SystemExit("Change Set failed; see reason above.")
aws("cloudformation", "execute-change-set", "--stack-name", stack_name, "--change-set-name", name)
waiter = "stack-create-complete" if kind == "CREATE" else "stack-update-complete"
aws("cloudformation", "wait", waiter, "--stack-name", stack_name)
print(f"Stack {stack_name} deployed successfully.")