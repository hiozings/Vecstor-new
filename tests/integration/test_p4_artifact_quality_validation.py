import json
from pathlib import Path
import runpy

import pytest

from vecstor.datasets import save_dataset
from test_p4_index_consistency import cluster, publish, load


def runner():
    return runpy.run_path(str(Path(__file__).resolve().parents[2]/'benchmarks/p4/validate_artifact_quality.py'))['run']


def test_short_evidence_cannot_export_profile(tmp_path):
    source = tmp_path/'cal'; source.mkdir()
    (source/'config.json').write_text(json.dumps({'calibration_count':2,'evaluation_count':3}))
    output = tmp_path/'out'
    with pytest.raises(ValueError, match='200|500'):
        runner()(source, '127.0.0.1:1', 'demo', 'items', output, 'cpu')
    assert not (output/'quality-profiles.json').exists()


def test_actual_metadata_fingerprint_is_verified_before_export(cluster, tmp_path):
    source = tmp_path/'cal'; scenario = source/'2048-independent'; scenario.mkdir(parents=True)
    (source/'config.json').write_text(json.dumps({'calibration_count':200,'evaluation_count':500}))
    save_dataset(cluster.data, scenario/'dataset.npz')
    cluster.clients['A'].execute(f'UPDATE items SET value=-1 WHERE id={cluster.update_id}')
    publish(cluster); load(cluster)
    output = tmp_path/'out'
    # SessionClient owns a real channel; channel target is exposed by the client address.
    address = cluster.clients['A'].address
    with pytest.raises(ValueError, match='dataset|fingerprint'):
        runner()(source, address, 'demo', 'items', output, 'cpu')
    assert not (output/'quality-profiles.json').exists()


def test_frozen_faiss_version_must_match_published_backend(cluster,tmp_path):
    source=tmp_path/'cal';scenario=source/'2048-independent';scenario.mkdir(parents=True)
    (source/'config.json').write_text(json.dumps({'calibration_count':200,'evaluation_count':500,'faiss':'0.0'}))
    save_dataset(cluster.data,scenario/'dataset.npz')
    publish(cluster);load(cluster)
    with pytest.raises(ValueError,match='Faiss'):
        runner()(source,cluster.clients['A'].address,'demo','items',tmp_path/'out','cpu')
