"""GPU isolation follows visible character devices, not CUDA ordinal names."""
from pathlib import Path

from vecstor import isolation


def test_gpu_devices_include_nonzero_container_node(tmp_path, monkeypatch):
    names={'null','nvidiactl','nvidia-uvm','nvidia-uvm-tools','nvidia6'}
    for name in names:
        (tmp_path/name).touch()
    # Creating character nodes requires CAP_MKNOD; simulate only their file type.
    monkeypatch.setattr(Path,'is_char_device',lambda p:p.name in names)
    paths=isolation._gpu_device_paths(tmp_path)
    assert {p.name for p in paths}==names


def test_gpu_devices_exclude_regular_files_and_unrelated_names(tmp_path, monkeypatch):
    for name in ('nvidia6','nvidia7','nvidia6-data','nvidia²','random','nvidia-caps'):
        (tmp_path/name).touch()
    monkeypatch.setattr(Path,'is_char_device',lambda p:p.name!='nvidia7')
    assert [p.name for p in isolation._gpu_device_paths(tmp_path)]==['nvidia6']


def test_gpu_devices_do_not_follow_symlinks(tmp_path, monkeypatch):
    target=tmp_path/'storage';target.touch()
    (tmp_path/'nvidia6').symlink_to(target)
    monkeypatch.setattr(Path,'is_char_device',lambda p:True)
    assert isolation._gpu_device_paths(tmp_path)==()
