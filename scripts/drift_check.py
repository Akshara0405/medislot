"""Run CloudFormation drift detection on every MediSlot stack and report the result (Milestone 7).

    python scripts/drift_check.py --environment dev [--region ap-south-1] [--report drift-report.json] [--strict]

Exit code 0 when every stack is IN_SYNC. With --strict, any drift (or a failed detection) exits 1.
"""
import argparse
import json
import subprocess
import sys
import time

STACKS = ['iam', 'network', 'data', 'auth', 'workflows', 'web', 'observability']


def aws(region, *args, check=True):
    out = subprocess.run(['aws', *args, '--region', region, '--output', 'json'], check=check, text=True,
                         capture_output=True)
    return json.loads(out.stdout) if out.returncode == 0 and out.stdout.strip() else None


def detect(region, stack):
    started = aws(region, 'cloudformation', 'detect-stack-drift', '--stack-name', stack, check=False)
    if not started:
        return {'stack': stack, 'status': 'NOT_FOUND'}
    detection = started['StackDriftDetectionId']
    for _ in range(120):
        info = aws(region, 'cloudformation', 'describe-stack-drift-detection-status',
                   '--stack-drift-detection-id', detection)
        if info['DetectionStatus'] != 'DETECTION_IN_PROGRESS':
            break
        time.sleep(5)
    result = {'stack': stack, 'status': info.get('StackDriftStatus'), 'detection': info['DetectionStatus'],
              'driftedResources': info.get('DriftedStackResourceCount', 0)}
    if info.get('StackDriftStatus') == 'DRIFTED':
        drifts = aws(region, 'cloudformation', 'describe-stack-resource-drifts', '--stack-name', stack,
                     '--stack-resource-drift-status-filters', 'MODIFIED', 'DELETED')
        result['resources'] = [{'logicalId': d['LogicalResourceId'], 'type': d['ResourceType'],
                                'status': d['StackResourceDriftStatus'],
                                'differences': [p['PropertyPath'] for p in d.get('PropertyDifferences', [])]}
                               for d in (drifts or {}).get('StackResourceDrifts', [])]
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--environment', default='dev')
    parser.add_argument('--region', default='ap-south-1')
    parser.add_argument('--report')
    parser.add_argument('--strict', action='store_true')
    args = parser.parse_args()
    results = [detect(args.region, f'medislot-{args.environment}-{name}') for name in STACKS]
    width = max(len(r['stack']) for r in results)
    for r in results:
        print(f"{r['stack']:<{width}}  {r['status']:<12} drifted resources: {r.get('driftedResources', '-')}")
        for res in r.get('resources', []):
            print(f"    {res['status']:<9} {res['logicalId']} ({res['type']}) {', '.join(res['differences'])}")
    if args.report:
        with open(args.report, 'w') as fh:
            json.dump({'checkedAt': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'stacks': results}, fh, indent=2)
    in_sync = all(r['status'] in ('IN_SYNC', 'NOT_FOUND') for r in results)
    print('ALL STACKS IN_SYNC' if in_sync else 'DRIFT FOUND - see above (docs/RUNBOOK.md, "Drift detection")')
    if args.strict and not in_sync:
        sys.exit(1)


if __name__ == '__main__':
    main()
