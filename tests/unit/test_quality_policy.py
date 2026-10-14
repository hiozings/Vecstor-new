from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path

import pytest
from vecstor.index.types import IndexKey
from vecstor.query.search_options import SearchOptions
from vecstor.query.statements import classify_statement


def document():
    return json.loads((Path(__file__).resolve().parents[2]/'configs/quality/recall95-example.json').read_text())


def setup():
    from vecstor.query.quality_policy import QualityCatalog
    d=document();p=d['profiles'][0];key=IndexKey(**p['index_key'])
    sql=f"SELECT id FROM items WHERE value < 1000 ORDER BY vector_distance(vector,{json.dumps([.5]*128)}) LIMIT 10"
    return QualityCatalog.from_document(d),p,key,classify_statement(sql).vector


def test_profile_applies_calibrated_floor_and_records_offline_basis():
    catalog,p,key,plan=setup()
    options=SearchOptions(mode='approx',quality_profile_id=p['profile_id'])
    resolved,description=catalog.resolve(p['profile_id'],key,256,100000,plan,options)
    assert resolved.nprobe==p['budget']['nprobe_floor']
    assert resolved.candidates==64
    assert description['state']=='applied' and description['profile_id']==p['profile_id']
    assert description['evaluation_mean_recall']>=.95
    assert 'online_recall' not in description


@pytest.mark.parametrize('changes',[dict(nprobe=1),dict(candidates=1),dict(max_candidates=1),dict(max_rounds=1)])
def test_explicit_budget_conflict_is_not_silently_ignored(changes):
    catalog,p,key,plan=setup()
    with pytest.raises(ValueError,match='conflict'):
        catalog.resolve(p['profile_id'],key,256,100000,plan,SearchOptions(mode='approx',**changes))


def test_profile_revision_and_statement_scope_must_match():
    from vecstor.query.quality_policy import QualityUnavailable
    catalog,p,key,plan=setup()
    with pytest.raises(QualityUnavailable,match='version'):
        catalog.resolve(p['profile_id'],replace(key,revision=key.revision+1),256,100000,plan,SearchOptions(mode='approx'))
    with pytest.raises(QualityUnavailable,match='scope'):
        catalog.resolve(p['profile_id'],key,256,100000,replace(plan,limit=11),SearchOptions(mode='approx'))
    with pytest.raises(QualityUnavailable,match='scope'):
        catalog.resolve(p['profile_id'],key,256,100000,replace(plan,query_vector=(10.,)*128),SearchOptions(mode='approx'))


@pytest.mark.parametrize('change',[{'evaluation_mean_recall':.94},{'evaluation_mean_recall':float('nan')},
                                  {'evaluation_queries':5},{'exact_queries':1},{'incomplete_queries':1}])
def test_invalid_or_fallback_contaminated_evidence_is_rejected(change):
    from vecstor.query.quality_policy import QualityCatalog
    d=document();d['profiles'][0]['evidence'].update(change)
    with pytest.raises(ValueError):QualityCatalog.from_document(d)


def test_duplicate_profile_ids_and_large_catalog_are_rejected():
    from vecstor.query.quality_policy import QualityCatalog
    d=document();d['profiles'].append(deepcopy(d['profiles'][0]))
    with pytest.raises(ValueError):QualityCatalog.from_document(d)
    d=document();d['profiles']*=129
    with pytest.raises(ValueError):QualityCatalog.from_document(d)


@pytest.mark.parametrize('change',[{'table_id':'recreated'},{'database_id':'other'},
                                  {'build_fingerprint':'f'*64},{'schema_version':'new-schema'},{'dimension':127}])
def test_profile_rejects_every_changed_snapshot_identity(change):
    from vecstor.query.quality_policy import QualityUnavailable
    catalog,p,key,plan=setup()
    with pytest.raises(QualityUnavailable,match='version'):
        catalog.resolve(p['profile_id'],replace(key,**change),256,100000,plan,SearchOptions(mode='approx'))
