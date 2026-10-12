import json

import numpy as np
import pytest


def sample(**changes):
    from vecstor.data import Dataset

    fields = dict(ids=[7, 2], categories=["论文", "O'Reilly"], timestamps=[-1, 1000],
                  values=[1.5, -2], vectors=[[1, 2], [3, 4]])
    fields.update(changes)
    return Dataset(**fields)


def test_data_owns_arrays_and_preserves_ids():
    original = np.array([[1, 2], [3, 4]], dtype=np.float64)
    dataset = sample(vectors=original)
    original[0, 0] = 999
    assert dataset.vectors.tolist() == [[1, 2], [3, 4]]
    assert dataset.vectors.dtype == np.float32
    assert dataset.ids.tolist() == [7, 2]
    assert dataset.dimension == 2
    with pytest.raises(ValueError):
        dataset.vectors[0, 0] = 5


@pytest.mark.parametrize("changes", [
    {"ids": [1, 1]}, {"ids": [-1, 2]}, {"ids": [1.5, 2]}, {"ids": [True, 2]},
    {"ids": [2**63, 2]}, {"ids": ["1", "2"]}, {"timestamps": [1.5, 2]},
    {"categories": [None, "x"]}, {"categories": ["", "x"]},
    {"categories": ["\x00", "x"]}, {"categories": ["x\x00", "x"]},
    {"values": [np.nan, 1]}, {"values": [np.inf, 1]}, {"values": ["1", "2"]},
    {"values": [True, 1]}, {"vectors": [[np.inf, 1], [1, 2]]},
    {"vectors": [[1e100, 1], [1, 2]]}, {"vectors": [[1, 2], [3]]},
    {"vectors": [["1", "2"], ["3", "4"]]}, {"vectors": [[True, 1], [1, 2]]},
    {"vectors": np.empty((2, 0))}, {"values": [1]},
])
def test_invalid_data_rejected(changes):
    from vecstor.data import DataError
    with pytest.raises(DataError):
        sample(**changes)


def test_empty_data_retains_dimension():
    from vecstor.data import Dataset
    data = Dataset([], [], [], [], np.empty((0, 3)))
    assert data.size == 0 and data.dimension == 3


def test_value_conversion_overflow_rejected():
    from vecstor.data import DataError
    with pytest.raises(DataError):
        sample(values=np.array([np.longdouble("1e400"), 1], dtype=np.longdouble))


def test_empty_generated_snapshot_roundtrip(tmp_path):
    from vecstor.datasets import generate_dataset, load_dataset, save_dataset
    data = generate_dataset(0, 3, distribution="correlated")
    path = tmp_path / "empty.npz"
    save_dataset(data, path)
    restored = load_dataset(path)
    assert restored.size == 0 and restored.dimension == 3


@pytest.mark.parametrize("distribution", ["independent", "correlated"])
def test_generator_reproducible_and_selectivity(distribution):
    from vecstor.datasets import generate_dataset
    a = generate_dataset(1000, 4, seed=42, distribution=distribution)
    b = generate_dataset(1000, 4, seed=42, distribution=distribution)
    np.testing.assert_array_equal(a.vectors, b.vectors)
    np.testing.assert_array_equal(a.values, b.values)
    assert len(np.unique(a.ids)) == 1000
    for threshold in [1, 10, 100, 500, 1000]:
        assert np.count_nonzero(a.values < threshold) == threshold
    corr = np.corrcoef(a.values, a.vectors[:, 0])[0, 1]
    assert corr > 0.95 if distribution == "correlated" else abs(corr) < 0.15


@pytest.mark.parametrize("args", [{"rows": -1}, {"rows": True}, {"dimension": 0},
                                  {"seed": -1}, {"distribution": "unknown"}])
def test_generator_bad_configuration(args):
    from vecstor.data import DataError
    from vecstor.datasets import generate_dataset
    with pytest.raises(DataError):
        generate_dataset(**args)


def test_snapshot_roundtrip_and_no_overwrite(tmp_path):
    from vecstor.datasets import save_dataset, load_dataset
    data = sample()
    path = tmp_path / "nested" / "sample.npz"
    save_dataset(data, path, metadata={"seed": 1})
    restored = load_dataset(path)
    for field in ["ids", "categories", "timestamps", "values", "vectors"]:
        np.testing.assert_array_equal(getattr(data, field), getattr(restored, field))
    with pytest.raises(FileExistsError):
        save_dataset(data, path)


def test_corrupt_manifest_rejected(tmp_path):
    from vecstor.data import DataError
    from vecstor.datasets import save_dataset, load_dataset
    path = tmp_path / "sample.npz"
    save_dataset(sample(), path)
    with np.load(path, allow_pickle=False) as source:
        arrays = dict(source)
    manifest = json.loads(arrays["manifest"].item())
    manifest["dimension"] = 100
    arrays["manifest"] = np.array(json.dumps(manifest))
    np.savez(path, **arrays)
    with pytest.raises(DataError):
        load_dataset(path)


def test_pickle_data_rejected(tmp_path):
    from vecstor.data import DataError
    from vecstor.datasets import load_dataset
    path = tmp_path / "object.npz"
    np.savez(path, ids=np.array([{}], dtype=object))
    with pytest.raises(DataError):
        load_dataset(path)
