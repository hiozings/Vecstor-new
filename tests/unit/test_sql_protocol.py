import pytest


def test_typed_result_preserves_bigint_null_vector_and_empty_result():
    from vecstor.rpc.sql_codec import encode_result, decode_frame
    result = {"columns": [{"name":"id","type":"BIGINT"},{"name":"v","type":"FLOAT[2]"},
                           {"name":"note","type":"VARCHAR"},{"name":"ok","type":"BOOLEAN"}],
              "rows": [[2**60, [0.25, 0.5], None, True]], "affected_rows":None, "revision":9}
    events = [decode_frame(frame) for frame in encode_result(result)]
    assert [e["kind"] for e in events] == ["schema","rows","complete"]
    assert events[1]["rows"] == result["rows"]
    assert events[2]["affected_rows"] is None
    assert events[2]["revision"] == 9
    assert [decode_frame(f)["kind"] for f in encode_result({"columns":[],"rows":[],"affected_rows":0})] == ["schema","complete"]


@pytest.mark.parametrize("value", [2**63, float("nan"), float("inf"), object()])
def test_unsupported_cells_rejected(value):
    from vecstor.rpc.sql_codec import encode_result
    with pytest.raises(ValueError):
        list(encode_result({"columns":[{"name":"a","type":"ANY"}],"rows":[[value]]}))


def test_batches_and_completion_metadata_roundtrip():
    from vecstor.rpc.sql_codec import encode_result, decode_frame
    result = {'columns':[{'name':'n','type':'BIGINT'}], 'rows':[[x] for x in range(513)],
              'affected_rows':513, 'operation_id':'op', 'revision':7,
              'selected_database_id':'db', 'selected_database_name':'demo', 'execution_path':'remote'}
    events = [decode_frame(frame) for frame in encode_result(result)]
    assert [len(e['rows']) for e in events if e['kind']=='rows'] == [256,256,1]
    assert events[-1]['selected_database_id'] == 'db'
    assert events[-1]['selected_database_name'] == 'demo'
    assert events[-1]['execution_path'] == 'remote'
    assert events[-1]['operation_id'] == 'op'
    assert events[-1]['affected_rows'] == 513


def test_message_size_and_row_width_limits():
    from vecstor.rpc.sql_codec import encode_result
    for rows in [[["x"*(4*1024*1024)]], [[1,2]]]:
        with pytest.raises(ValueError):
            list(encode_result({'columns':[{'name':'n','type':'VARCHAR'}], 'rows':rows}))


@pytest.mark.parametrize('cell', ['unset', 'nan', 'vector_nan', 'null_false'])
def test_malformed_wire_cells_rejected(cell):
    from vecstor.rpc.sql_codec import decode_frame
    from vecstor.rpc import sql_pb2 as pb
    values = {'unset':pb.Cell(), 'nan':pb.Cell(number=float('nan')),
              'vector_nan':pb.Cell(vector=pb.VectorValue(values=[float('nan')])),
              'null_false':pb.Cell(null_value=False)}
    with pytest.raises(ValueError):
        decode_frame(pb.ResultFrame(kind=pb.ResultFrame.ROWS, rows=[pb.SQLRow(cells=[values[cell]])]))


def test_vector_numeric_overflow_is_value_error():
    from vecstor.rpc.sql_codec import encode_result
    with pytest.raises(ValueError):
        list(encode_result({'columns':[{'name':'v','type':'FLOAT[1]'}], 'rows':[[[10**1000]]]}))
