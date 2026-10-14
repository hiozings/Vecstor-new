import pytest
from vecstor.rpc import sql_pb2 as pb
from vecstor.query import search_options


def test_defaults_and_proto_presence():
    options = search_options.SearchOptions()
    assert options.validate() is options
    assert search_options.SearchOptions.from_proto(pb.SearchOptions()) == options
    assert search_options.SearchOptions.from_proto(options.to_proto()) == options
    with pytest.raises(ValueError):
        search_options.SearchOptions.from_proto(pb.SearchOptions(nprobe=0))


@pytest.mark.parametrize('changes', [dict(mode='bad'), dict(path='search-first'), dict(nprobe=True), dict(candidates=0), dict(max_candidates=2**32), dict(max_rounds=33), dict(fallback='bad')])
def test_reject_options(changes):
    with pytest.raises(ValueError):
        search_options.SearchOptions(**changes).validate()


def test_approximate_roundtrip():
    options = search_options.SearchOptions(mode='approx', path='search-first', nprobe=4, candidates=64, max_candidates=512, max_rounds=4, fallback='error')
    assert search_options.SearchOptions.from_proto(options.to_proto()) == options
