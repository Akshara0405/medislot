"""Guards on the CloudFormation templates: what the slot code needs must actually be deployed and allowed."""
import os, re
import yaml

ROOT = os.path.join(os.path.dirname(__file__), '..')


class Loader(yaml.SafeLoader): pass
Loader.add_multi_constructor('!', lambda loader, suffix, node: None)


def template():
    with open(os.path.join(ROOT, 'cloudformation', 'auth-stack.yaml')) as f:
        return yaml.load(f, Loader=Loader)['Resources']


def test_materializer_lambda_is_defined_with_a_daily_schedule():
    res = template()
    assert res['MaterializerFunction']['Type'] == 'AWS::Lambda::Function'
    assert res['MaterializerFunction']['Properties']['Handler'] == 'lambda_function.handler'
    assert res['MaterializerSchedule']['Type'] == 'AWS::Events::Rule'
    assert res['MaterializerSchedulePermission']['Properties']['Principal'] == 'events.amazonaws.com'


def test_every_packaged_lambda_has_a_function_in_the_stack():
    workflow = open(os.path.join(ROOT, '.github', 'workflows', 'deploy.yaml')).read()
    packaged = re.search(r'for fn in ([a-z ]+); do', workflow).group(1).split()
    text = open(os.path.join(ROOT, 'cloudformation', 'auth-stack.yaml')).read()
    for fn in packaged:
        assert f"/{fn}.zip" in text, f'{fn} is packaged by the workflow but no function uses it'


def allowed_actions(role):
    out = set()
    for pol in template()[role]['Properties']['Policies']:
        for st in pol['PolicyDocument']['Statement']:
            out.update(st['Action'])
    return out


def test_doctor_role_can_update_slot_status():
    # resync_slots flips booked slots to CLOSED / back with update_item
    assert 'dynamodb:UpdateItem' in allowed_actions('DoctorRole')


def test_materializer_role_can_read_and_create_slots():
    assert {'dynamodb:Query', 'dynamodb:PutItem'} <= allowed_actions('MaterializerRole')
