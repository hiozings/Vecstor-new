"""Validated, owned arrays for the local reference dataset."""

from dataclasses import dataclass
import numbers

import numpy as np


class DataError(ValueError):
    """Dataset or query vector violates the data contract."""


def numeric_array(value, name):
    try:
        if not isinstance(value, np.ndarray):
            objects = np.asarray(value, dtype=object)
            if any(isinstance(x, (bool, np.bool_)) or not isinstance(x, numbers.Real)
                   for x in objects.flat):
                raise DataError(f"{name} must contain real numbers, not booleans or strings")
        array = np.asarray(value)
        if array.dtype.kind not in "iuf":
            raise DataError(f"{name} must contain real numbers")
        if not np.isfinite(array).all():
            raise DataError(f"{name} must be finite")
        return array
    except (TypeError, ValueError, OverflowError) as error:
        raise DataError(f"Invalid {name}: {error}") from error


def integer_array(value, name):
    array = numeric_array(value, name)
    if array.ndim != 1:
        raise DataError(f"{name} must be one-dimensional")
    if array.size and array.dtype.kind not in "iu":
        raise DataError(f"{name} must contain integers")
    if any(not -(2**63) <= int(x) < 2**63 for x in array):
        raise DataError(f"{name} is outside int64 range")
    return array.astype(np.int64, copy=True)


def float32_array(value, name):
    array = numeric_array(value, name)
    with np.errstate(over="ignore", invalid="ignore"):
        result = array.astype(np.float32, copy=True)
    if not np.isfinite(result).all():
        raise DataError(f"{name} overflows float32")
    return result


def query_vector(value, dimension):
    result = float32_array(value, "query vector")
    if result.shape != (dimension,):
        raise DataError(f"Query vector must have shape ({dimension},), got {result.shape}")
    return result


@dataclass(frozen=True)
class Dataset:
    ids: np.ndarray
    categories: np.ndarray
    timestamps: np.ndarray
    values: np.ndarray
    vectors: np.ndarray

    def __post_init__(self):
        ids = integer_array(self.ids, "ids")
        if (ids < 0).any() or np.unique(ids).size != ids.size:
            raise DataError("ids must be nonnegative and unique")
        timestamps = integer_array(self.timestamps, "timestamps")
        raw_categories = np.asarray(self.categories, dtype=object)
        if raw_categories.ndim != 1 or any(not isinstance(x, str) or not x or "\x00" in x for x in raw_categories):
            raise DataError("categories must contain nonempty strings without NUL characters")
        categories = raw_categories.astype(str)
        with np.errstate(over="ignore", invalid="ignore"):
            values = numeric_array(self.values, "values").astype(np.float64, copy=True)
        if not np.isfinite(values).all():
            raise DataError("values overflows float64")
        vectors = float32_array(self.vectors, "vectors")
        if vectors.ndim != 2 or vectors.shape[1] == 0:
            raise DataError("vectors must have shape (N, D), with D > 0")
        if values.ndim != 1 or any(len(x) != len(ids) for x in (categories, timestamps, values, vectors)):
            raise DataError("All fields must have the same row count")
        for name, array in dict(ids=ids, categories=categories, timestamps=timestamps,
                                values=values, vectors=vectors).items():
            array.setflags(write=False)
            object.__setattr__(self, name, array)

    @property
    def size(self):
        return len(self.ids)

    @property
    def dimension(self):
        return self.vectors.shape[1]
