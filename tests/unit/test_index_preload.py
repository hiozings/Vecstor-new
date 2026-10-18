import json
import pytest

from vecstor.compute.index_recovery import read_preload


def test_preload_config_rejects_duplicates_unknown_keys_and_devices(tmp_path):
    file=tmp_path/'preload.json'
    target=dict(database='demo',table='items',device='cpu')
    for value in [dict(format_version=1,targets=[target,target]),
                  dict(format_version=1,targets=[dict(target,device='auto')]),
                  dict(format_version=1,targets=[dict(target,path='/secret')])]:
        file.write_text(json.dumps(value))
        with pytest.raises(ValueError):read_preload(file,None)
    file.write_text(json.dumps(dict(format_version=1,targets=[target])))
    assert read_preload(file,None)==[target]


def test_preload_duplicates_use_logical_case_insensitive_names(tmp_path):
    file=tmp_path/'preload.json'
    file.write_text(json.dumps(dict(format_version=1,targets=[dict(database='DEMO',table='ITEMS',device='gpu'),
                                                            dict(database='demo',table='items',device='cpu')])))
    with pytest.raises(ValueError,match='Duplicate'):read_preload(file,None)


def test_preload_normalizes_logical_names(tmp_path):
    file=tmp_path/'preload.json'
    file.write_text(json.dumps(dict(format_version=1,targets=[dict(database='DEMO',table='ITEMS',device='cpu')])))
    assert read_preload(file,None)==[dict(database='demo',table='items',device='cpu')]


def test_preload_config_respects_size_and_storage_boundary(tmp_path):
    root=tmp_path/'storage';root.mkdir()
    file=root/'preload.json';file.write_text('{}')
    with pytest.raises(ValueError):read_preload(file,root)
    file=tmp_path/'large.json';file.write_bytes(b' '*(1024**2+1))
    with pytest.raises(ValueError):read_preload(file,None)
