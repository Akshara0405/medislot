"""Print the patient Lambda's explain-search result as a readable table and fail CI if an index is missing.

Usage: python scripts/explain_report.py /tmp/explain-search.json
"""
import json
import sys


def main(path):
    with open(path) as handle:
        result = json.load(handle)
    if 'errorMessage' in result:
        print('::error::explain-search failed: ' + result['errorMessage'])
        return 1
    body = result.get('body', result)
    if isinstance(body, str):
        body = json.loads(body)

    print(f"Doctors in table: {body.get('doctorRows')}")
    print(body.get('note', ''))
    print()
    header = f"{'filters':<45} {'access':<12} {'index used':<28} {'rows':>6}  extra"
    print(header)
    print('-' * len(header))
    for plan in body.get('plans', []):
        step = plan.get('doctorsStep') or {}
        print(f"{' + '.join(plan['filters']):<45} {str(step.get('type')):<12} {str(step.get('key')):<28} "
              f"{str(step.get('rows')):>6}  {step.get('extra') or ''}")
        print(f"{'':<45} candidates: {step.get('possibleKeys')}")
    print()
    for table, indexes in sorted((body.get('indexes') or {}).items()):
        for name, columns in sorted(indexes.items()):
            print(f'{table}.{name}: ({", ".join(columns)})')

    problems = (body.get('missingIndexes') or []) + (body.get('missingColumns') or [])
    if problems:
        for item in problems:
            print(f'::error::Missing in RDS: {item}. ensure_milestone4_schema() in lambdas/common.py should have added it - check the Lambda logs for schema_upgraded / errors.')
        return 1
    print('\nAll Milestone 4 indexes and columns are present.')
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1]))
