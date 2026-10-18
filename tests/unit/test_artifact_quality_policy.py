from dataclasses import asdict, replace
import json
from pathlib import Path

import faiss
import pytest

from vecstor.query.quality_policy import QualityCatalog, QualityUnavailable
from vecstor.query.search_options import SearchOptions
from vecstor.query.statements import classify_statement


def fixture(descriptor, version=3):
    document = json.loads((Path(__file__).resolve().parents[2]/'configs/quality/recall95-example.json').read_text())
    profile = document['profiles'][0]
    document['format_version'] = profile['format_version'] = version
    profile['index_key'] = asdict(descriptor.key)
    sql = 'SELECT id FROM items WHERE value<2000 ORDER BY vector_distance(vector,[0.5,0.5]) LIMIT 10'
    plan = classify_statement(sql).vector
    profile['workload'].update(metadata_sql=plan.metadata_sql, path='search-first')
    profile['budget'].update(nprobe_floor=1, initial_candidates=64, max_candidates=2048)
    profile['execution'] = dict(device='cpu', precision='float32', faiss_version=faiss.__version__, gpu_model='not_applicable')
    if version == 3:
        profile['artifact_identity'] = {name:getattr(descriptor,name) for name in ('artifact_id','payload_sha256','snapshot_sha256')}
    return QualityCatalog.from_document(document), profile, plan


@pytest.mark.parametrize('field', ['artifact_id','payload_sha256','snapshot_sha256'])
def test_same_key_wrong_artifact_profile_rejected(cpu_artifact, field):
    descriptor = cpu_artifact[0]
    catalog, profile, plan = fixture(descriptor)
    options = SearchOptions(mode='approx', path='search-first')
    with pytest.raises(QualityUnavailable, match='artifact'):
        catalog.resolve(profile['profile_id'], descriptor.key, 1, 2048, plan, options,
                        artifact=replace(descriptor, **{field:'0'*64}), index_source='published')
    effective, description = catalog.resolve(profile['profile_id'], descriptor.key, 1, 2048, plan, options,
                        artifact=descriptor, index_source='published')
    assert effective.nprobe == 1
    assert description['artifact_identity']['artifact_id'] == descriptor.artifact_id


def test_v2_profile_rejected_only_in_published_mode(cpu_artifact):
    descriptor = cpu_artifact[0]
    catalog, profile, plan = fixture(descriptor, version=2)
    options = SearchOptions(mode='approx', path='search-first')
    assert catalog.resolve(profile['profile_id'], descriptor.key, 1, 2048, plan, options)[0].nprobe == 1
    with pytest.raises(QualityUnavailable, match='unbound'):
        catalog.resolve(profile['profile_id'], descriptor.key, 1, 2048, plan, options,
                        artifact=descriptor, index_source='published')
