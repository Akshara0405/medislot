"""Tear down every MediSlot stack of one environment (Milestone 7 "teardown and full redeploy from scratch").

    python scripts/teardown.py --environment dev --confirm DELETE-medislot-dev [--region ap-south-1]

Order (reverse of deployment): observability, web, workflows, auth, data, network. The iam stack is KEPT: it holds
the GitHub Actions deployment policy, and deleting it would remove the pipeline's own permissions (the redeploy
would then need a manual step). The SSM SecureString secrets are kept too, so the redeploy reuses them.

Before the data stack is deleted, its two versioned S3 buckets are emptied (every object version and delete marker);
CloudFormation cannot delete a bucket that is not empty. RDS takes a final snapshot (DeletionPolicy: Snapshot).
After this, run the "MediSlot CI and Deployment" workflow (workflow_dispatch) to rebuild everything.
"""
import argparse
import json
import subprocess
import sys

ORDER = ['observability', 'web', 'workflows', 'auth', 'data', 'network']


def aws(region, *args, check=True):
    out = subprocess.run(['aws', *args, '--region', region, '--output', 'json'], text=True, capture_output=True)
    if out.returncode and check:
        raise SystemExit(out.stderr)
    return json.loads(out.stdout) if out.returncode == 0 and out.stdout.strip() else None


def empty_bucket(region, bucket):
    while True:
        page = aws(region, 's3api', 'list-object-versions', '--bucket', bucket, '--max-items', '1000', check=False)
        if not page:
            return
        objects = [{'Key': o['Key'], 'VersionId': o['VersionId']}
                   for o in page.get('Versions', []) + page.get('DeleteMarkers', [])]
        if not objects:
            return
        aws(region, 's3api', 'delete-objects', '--bucket', bucket,
            '--delete', json.dumps({'Objects': objects, 'Quiet': True}))
        print(f'  emptied {len(objects)} object versions from {bucket}')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--environment', default='dev')
    p.add_argument('--region', default='ap-south-1')
    p.add_argument('--confirm', required=True)
    a = p.parse_args()
    if a.confirm != f'DELETE-medislot-{a.environment}':
        sys.exit(f'Refusing: --confirm must be DELETE-medislot-{a.environment}')
    account = aws(a.region, 'sts', 'get-caller-identity')['Account']
    for name in ORDER:
        stack = f'medislot-{a.environment}-{name}'
        if not aws(a.region, 'cloudformation', 'describe-stacks', '--stack-name', stack, check=False):
            print(f'{stack}: not deployed, skipping')
            continue
        if name == 'data':
            for bucket in (f'medislot-{a.environment}-artifacts-{account}',
                           f'medislot-{a.environment}-doctor-photos-{account}-{a.region}'):
                empty_bucket(a.region, bucket)
        print(f'{stack}: deleting ...')
        aws(a.region, 'cloudformation', 'delete-stack', '--stack-name', stack)
        done = subprocess.run(['aws', 'cloudformation', 'wait', 'stack-delete-complete', '--stack-name', stack,
                               '--region', a.region])
        if done.returncode:
            sys.exit(f'{stack}: delete failed - see the stack events in the CloudFormation console')
        print(f'{stack}: deleted')
    print('Teardown complete. Kept: medislot-%s-iam stack, SSM secrets, the final RDS snapshot.' % a.environment)


if __name__ == '__main__':
    main()
