"""Read-only IVF handles; all search tuning belongs to individual requests."""
from io import BytesIO

import faiss
import numpy as np

from .types import BuildConfig


def _ids(values):
    array = np.asarray(values)
    if array.ndim != 1 or array.dtype != np.dtype('int64') or np.any(array < 0):
        raise ValueError('ids must be nonnegative int64 values')
    return np.ascontiguousarray(array)


def _safe(dimension, max_abs):
    if max_abs > (.25 * float(np.finfo(np.float32).max) / (4.0 * dimension)) ** .5:
        raise ValueError('unsafe float32 distance range')


class FaissIndex:
    def __init__(self, index, max_abs):
        self._index = index
        if (not isinstance(index, faiss.IndexIVFFlat) or
                index.metric_type != faiss.METRIC_L2 or not index.is_trained or
                index.d <= 0 or index.nlist <= 0 or index.ntotal <= 0 or
                index.code_size != index.d * 4 or index.quantizer.d != index.d or
                index.quantizer.ntotal != index.nlist):
            raise ValueError('invalid IVF structure')
        self.nlist = int(index.nlist)
        self.row_count = int(index.ntotal)
        self.dimension = int(index.d)
        all_ids = []
        maximum = 0.0
        lists = index.invlists
        if lists.nlist != self.nlist or lists.code_size != index.code_size:
            raise ValueError('invalid IVF inverted lists')
        for bucket in range(self.nlist):
            count = lists.list_size(bucket)
            if not count:
                continue
            ids_pointer = lists.get_ids(bucket)
            try:
                all_ids.append(faiss.rev_swig_ptr(ids_pointer, count).copy())
            finally:
                lists.release_ids(bucket, ids_pointer)
            codes_pointer = lists.get_codes(bucket)
            try:
                vectors = faiss.rev_swig_ptr(codes_pointer, count * index.code_size).view(np.float32)
                if not np.isfinite(vectors).all():
                    raise ValueError('nonfinite stored IVF vectors')
                maximum = max(maximum, float(np.abs(vectors).max()))
            finally:
                lists.release_codes(bucket, codes_pointer)
        actual_ids = np.concatenate(all_ids) if all_ids else np.empty(0, dtype=np.int64)
        if (actual_ids.size != self.row_count or np.any(actual_ids < 0) or
                np.unique(actual_ids).size != self.row_count):
            raise ValueError('invalid IVF id identity or count')
        actual_ids.sort()
        actual_ids.setflags(write=False)
        self.ids = actual_ids
        # Derive this bound from the actual codes, never trust archive metadata.
        self.max_abs = maximum
        _safe(self.dimension, maximum)
        # Logical serialized footprint, not a native allocator/RSS hard bound.
        self.byte_size = int(faiss.serialize_index(index).nbytes)

    @classmethod
    def build(cls, ids, vectors, config: BuildConfig):
        ids = _ids(ids)
        vectors = np.asarray(vectors)
        if (vectors.ndim != 2 or vectors.dtype != np.dtype('float32') or
                vectors.shape[0] != ids.size or vectors.shape[1] < 1 or
                not np.isfinite(vectors).all() or np.unique(ids).size != ids.size):
            raise ValueError('invalid vectors or duplicate ids')
        config = config.resolved(int(ids.size))
        max_abs = float(np.abs(vectors).max())
        _safe(vectors.shape[1], max_abs)
        order = np.argsort(ids)
        vectors = np.ascontiguousarray(vectors[order])
        ids = np.ascontiguousarray(ids[order])
        index = faiss.IndexIVFFlat(faiss.IndexFlatL2(vectors.shape[1]), vectors.shape[1], config.nlist)
        index.cp.seed = config.seed
        sample = vectors
        if ids.size > config.train_rows:
            selected = np.sort(np.random.default_rng(config.seed).choice(ids.size, config.train_rows, replace=False))
            sample = np.ascontiguousarray(vectors[selected])
        index.train(sample)
        index.add_with_ids(vectors, ids)
        if index.ntotal != ids.size:
            raise ValueError('incomplete index build')
        return cls(index, max_abs)

    def search(self, query, count, nprobe, allowed_ids=None):
        query = np.asarray(query)
        if query.shape != (self.dimension,) or not np.isfinite(query).all():
            raise ValueError('invalid query vector')
        _safe(self.dimension, max(self.max_abs, float(np.abs(query.astype(np.float64)).max())))
        if isinstance(count, bool) or not isinstance(count, int) or count < 1:
            raise ValueError('count must be positive')
        if isinstance(nprobe, bool) or not isinstance(nprobe, int) or not 1 <= nprobe <= self.nlist:
            raise ValueError('nprobe outside index range')
        if allowed_ids is not None:
            allowed = _ids(allowed_ids)
            selector = faiss.IDSelectorBatch(allowed)
            # Faiss' keyword constructor preserves SWIG caller ownership.
            # Assigning params.sel afterwards transfers thisown=False and leaks
            # the selector because SearchParameters does not own/delete it.
            params = faiss.SearchParametersIVF(nprobe=nprobe, sel=selector)
        else:
            params = faiss.SearchParametersIVF(nprobe=nprobe)
        distances, ids = self._index.search(np.ascontiguousarray(query, dtype=np.float32).reshape(1, -1), min(count, self.row_count), params=params)
        valid = ids[0] != -1
        if not np.isfinite(distances[0][valid]).all():
            raise ValueError('nonfinite ANN distance')
        return distances[0][valid], ids[0][valid]

    def serialize(self):
        stream = BytesIO()
        np.savez(stream, index=faiss.serialize_index(self._index), max_abs=np.array(self.max_abs))
        return stream.getvalue()

    @classmethod
    def deserialize(cls, data):
        try:
            with np.load(BytesIO(data), allow_pickle=False) as archive:
                index = faiss.deserialize_index(archive['index'])
                maximum = float(archive['max_abs'])
            if not isinstance(index, faiss.IndexIVFFlat) or index.metric_type != faiss.METRIC_L2 or not np.isfinite(maximum) or maximum < 0:
                raise ValueError('invalid index metadata')
            _safe(index.d, maximum)
            return cls(index, maximum)
        except (RuntimeError, KeyError, TypeError) as exc:
            raise ValueError('invalid serialized index') from exc
