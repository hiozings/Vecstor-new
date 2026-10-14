"""Typed SQL results with bounded Protobuf frames and lossless int64 cells."""
import math
import json

from vecstor.rpc import sql_pb2 as pb
from vecstor.rpc.common import MAX_MESSAGE_BYTES

BATCH_ROWS = 256


def _bounded(frame):
    if frame.ByteSize() > MAX_MESSAGE_BYTES:
        raise ValueError('SQL result message exceeds 4 MiB')
    return frame


def _cell(value):
    if value is None:
        return pb.Cell(null_value=True)
    if isinstance(value, bool):
        return pb.Cell(boolean=value)
    if isinstance(value, int):
        if not -(2**63) <= value < 2**63:
            raise ValueError('Integer cell is outside int64 range')
        return pb.Cell(integer=value)
    if isinstance(value, float) and math.isfinite(value):
        return pb.Cell(number=value)
    if isinstance(value, str):
        return pb.Cell(text=value)
    if isinstance(value, (tuple, list)):
        try:
            if any(isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x) for x in value):
                raise ValueError('Vector cells require finite numeric values')
            return pb.Cell(vector=pb.VectorValue(values=value))
        except OverflowError as error:
            raise ValueError('Vector cell component overflows float64') from error
    raise ValueError(f'Unsupported SQL cell type or nonfinite value: {type(value).__name__}')


def encode_result(result):
    """Yield schema, bounded row batches, and exactly one completion frame."""
    columns = [pb.Column(name=column['name'], type=column['type']) for column in result.get('columns', [])]
    yield _bounded(pb.ResultFrame(kind=pb.ResultFrame.SCHEMA, columns=columns))
    batch = []
    byte_estimate = 2
    for values in result.get('rows', []):
        if len(values) != len(columns):
            raise ValueError('SQL row width differs from schema')
        row = pb.SQLRow(cells=[_cell(value) for value in values])
        # Five bytes safely bound a length prefix for a message up to 4 MiB.
        size = row.ByteSize() + 5
        if size + 2 > MAX_MESSAGE_BYTES:
            _bounded(pb.ResultFrame(kind=pb.ResultFrame.ROWS, rows=[row]))
        if batch and (len(batch) >= BATCH_ROWS or byte_estimate + size > MAX_MESSAGE_BYTES):
            yield _bounded(pb.ResultFrame(kind=pb.ResultFrame.ROWS, rows=batch))
            batch, byte_estimate = [], 2
        batch.append(row)
        byte_estimate += size
    if batch:
        yield _bounded(pb.ResultFrame(kind=pb.ResultFrame.ROWS, rows=batch))
    completion = pb.ResultFrame(kind=pb.ResultFrame.COMPLETE,
                               revision=result.get('revision', 0),
                               operation_id=result.get('operation_id', ''),
                               elapsed_seconds=result.get('elapsed_seconds', 0),
                               selected_database_id=result.get('selected_database_id', ''),
                               selected_database_name=result.get('selected_database_name', ''),
                               execution_path=result.get('execution_path', ''))
    if 'execution_plan' in result:
        plan = result['execution_plan']
        if not isinstance(plan, dict):
            raise ValueError('Execution plan must be a JSON object')
        completion.execution_plan.json_payload = json.dumps(plan, allow_nan=False)
    affected = result.get('affected_rows')
    if affected is not None:
        if isinstance(affected, bool) or not isinstance(affected, int) or not 0 <= affected < 2**63:
            raise ValueError('Affected row count must be a nonnegative int64')
        completion.affected_rows = affected
    if not math.isfinite(completion.elapsed_seconds) or completion.elapsed_seconds < 0:
        raise ValueError('Elapsed time must be finite and nonnegative')
    yield _bounded(completion)


def _value(cell):
    kind = cell.WhichOneof('value')
    if kind == 'null_value':
        if not cell.null_value:
            raise ValueError('Malformed NULL cell')
        return None
    if kind in {'boolean', 'integer', 'text'}:
        return getattr(cell, kind)
    if kind == 'number' and math.isfinite(cell.number):
        return cell.number
    if kind == 'vector' and all(math.isfinite(x) for x in cell.vector.values):
        return list(cell.vector.values)
    raise ValueError('Missing or nonfinite SQL cell')


def decode_frame(frame):
    """Decode one frame; callers enforce ordering and cross-frame schema width."""
    _bounded(frame)
    if frame.kind != pb.ResultFrame.COMPLETE and (
            frame.HasField('affected_rows') or frame.operation_id or frame.revision or
            frame.elapsed_seconds or frame.selected_database_id or frame.selected_database_name or frame.execution_path or frame.HasField('execution_plan')):
        raise ValueError('Completion metadata appears in a non-completion frame')
    if frame.kind == pb.ResultFrame.SCHEMA:
        if frame.rows:
            raise ValueError('Schema frame contains rows')
        return {'kind':'schema', 'columns':[{'name':x.name, 'type':x.type} for x in frame.columns]}
    if frame.kind == pb.ResultFrame.ROWS:
        if frame.columns or not frame.rows or len(frame.rows) > BATCH_ROWS:
            raise ValueError('Malformed row batch')
        return {'kind':'rows', 'rows':[[_value(cell) for cell in row.cells] for row in frame.rows]}
    if frame.kind == pb.ResultFrame.COMPLETE:
        if frame.columns or frame.rows or not math.isfinite(frame.elapsed_seconds) or frame.elapsed_seconds < 0:
            raise ValueError('Malformed completion frame')
        affected = frame.affected_rows if frame.HasField('affected_rows') else None
        if affected is not None and affected < 0:
            raise ValueError('Negative affected row count')
        event = {'kind':'complete', 'affected_rows':affected, 'revision':frame.revision,
                'operation_id':frame.operation_id, 'elapsed_seconds':frame.elapsed_seconds,
                'selected_database_id':frame.selected_database_id,
                'selected_database_name':frame.selected_database_name,
                'execution_path':frame.execution_path}
        if frame.HasField('execution_plan'):
            plan = json.loads(frame.execution_plan.json_payload)
            if not isinstance(plan, dict):
                raise ValueError('Execution plan must be a JSON object')
            json.dumps(plan, allow_nan=False)
            event['execution_plan'] = plan
        return event
    raise ValueError('Unknown result frame kind')
