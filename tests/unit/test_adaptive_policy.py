"""Version/context mismatches must not silently transfer planning evidence."""
from dataclasses import FrozenInstanceError, replace
import json

import pytest

from vecstor.cache.types import DataScope
from vecstor.compute.planner_types import DeploymentContext, PlanningInputs, PlanCandidate
from vecstor.query.adaptive_policy import AdaptivePolicy
from vecstor.query.search_options import SearchOptions

SCOPE = DataScope('epoch', 'db', 'table', 1, 'schema', 4)
CTX = DeploymentContext('hardware', 'loopback', 1)


def match(policy, document, scope=SCOPE, context=CTX, artifact=None):
    work = document['workloads'][0]
    return policy.lookup(scope, artifact or work['artifact_identity'], work['metadata_sql'], 10, context)


def test_cost_context_mismatch_does_not_erase_quality_mapping(adaptive_document):
    policy = AdaptivePolicy.from_document(adaptive_document)
    good = match(policy, adaptive_document)
    assert good.matched_rows == 100 and good.cost_reason is None
    assert len(good.models) == 1
    for context in (replace(CTX, network_class='remote'), replace(CTX, hardware_fingerprint=None),
                    replace(CTX, active_vector_requests=9)):
        bad = match(policy, adaptive_document, context=context)
        assert bad.matched_rows == 100
        assert bad.profiles['filter-first'] == 'filter_profile'
        assert bad.models == () and bad.cost_reason == 'deployment_context_mismatch'


def test_persistent_stats_rebind_epoch_only_after_identity_match(adaptive_document):
    policy = AdaptivePolicy.from_document(adaptive_document)
    assert match(policy, adaptive_document, replace(SCOPE, storage_epoch='restart')).matched_rows == 100
    assert match(policy, adaptive_document, replace(SCOPE, revision=2)).matched_rows is None
    artifact = dict(adaptive_document['workloads'][0]['artifact_identity'], artifact_id='new')
    changed = match(policy, adaptive_document, artifact=artifact)
    assert changed.matched_rows == 100 and not changed.profiles and not changed.models
    assert changed.cost_reason == 'artifact_mismatch'


@pytest.mark.parametrize('change', ['size','workloads','sql','nan','negative','duplicate','unknown','bad_count','bad_digest'])
def test_policy_rejects_oversize_nonfinite_negative_and_duplicate(adaptive_document, tmp_path, change):
    doc = adaptive_document
    work = doc['workloads'][0]
    if change == 'size':
        path = tmp_path/'large.json'; path.write_bytes(b' '*(2*1024**2+1))
        with pytest.raises(ValueError): AdaptivePolicy.load(path)
        return
    if change == 'workloads':doc['workloads'] *= 65
    if change == 'sql':work['metadata_sql'] = 'x'*65537
    if change == 'nan':work['models'][0]['coefficients']['intercept'] = float('nan')
    if change == 'negative':work['models'][0]['coefficients']['payload_bytes'] = -1
    if change == 'duplicate':doc['workloads'] *= 2
    if change == 'unknown':work['models'][0]['coefficients']['python_eval'] = 1
    if change == 'bad_count':work['matched_rows'] = 1001
    if change == 'bad_digest':work['artifact_identity']['payload_sha256'] = 'z'*64
    with pytest.raises(ValueError):AdaptivePolicy.from_document(doc)


def test_duplicate_json_key_and_roundtrip(adaptive_document, tmp_path):
    path = tmp_path/'policy.json'; path.write_text(json.dumps(adaptive_document))
    policy = AdaptivePolicy.load(path)
    assert policy.policy_hash == AdaptivePolicy.from_document(adaptive_document).policy_hash
    path.write_text('{"format_version":1,"format_version":1,"policy_id":"x","workloads":[]}')
    with pytest.raises(ValueError, match='Duplicate'):AdaptivePolicy.load(path)


def test_planner_inputs_are_detached_and_frozen(adaptive_document):
    policy = AdaptivePolicy.from_document(adaptive_document)
    adaptive_document['workloads'][0]['profiles']['filter-first'] = 'mutated'
    assert policy.describe()['workload_count'] == 1
    candidate = PlanCandidate('exact', 'exact', 'filter-first', SearchOptions(), None, 'f'*64)
    statistics = {'cardinality': {'value': 100}, 'samples': [1, 2]}
    inputs = PlanningInputs(SCOPE, {}, 1000, 4, 10, 'a'*64, 'b'*64,
        (candidate,), 'exact', statistics, {}, CTX, policy.policy_id, policy.policy_hash, 1000.)
    statistics['cardinality']['value'] = 0
    assert inputs.statistics['cardinality']['value'] == 100
    with pytest.raises(TypeError): inputs.statistics['cardinality']['value'] = 1
    with pytest.raises(FrozenInstanceError): inputs.k = 2
    assert inputs.statistics['samples'] == (1, 2)


def test_planning_values_reject_mutable_arrays_and_objects():
    import numpy as np
    from vecstor.compute.planner_types import freeze
    for value in (np.zeros(2), object(), {1, 2}):
        with pytest.raises(TypeError, match='planning value'):
            freeze({'nested': value})


def test_policy_parameters_are_frozen_and_validated(adaptive_document):
    adaptive_document['parameters']={'filter_workspace_bytes':12345,'switch_min_ms':3.}
    policy=AdaptivePolicy.from_document(adaptive_document)
    assert policy.parameters['filter_workspace_bytes']==12345
    assert policy.parameters['risk_limit']==.01
    with pytest.raises(TypeError):policy.parameters['switch_min_ms']=1
    adaptive_document['parameters']['switch_margin']=float('inf')
    with pytest.raises(ValueError):AdaptivePolicy.from_document(adaptive_document)


def test_arbitrary_frozen_wrapper_cannot_hide_mutable_planning_state():
    from dataclasses import dataclass
    from vecstor.compute.planner_types import freeze
    @dataclass(frozen=True)
    class Wrapper:
        mutable: list
    with pytest.raises(TypeError):freeze(Wrapper([1]))
