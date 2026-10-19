"""Strict presence, ordering and payload checks for projected read responses."""
import math

import grpc
import numpy as np

from vecstor.query.read_projection import validate_fields
from vecstor.rpc.common import RemoteError, VECTOR_PAYLOAD_BYTES, MAX_MESSAGE_BYTES


def require(condition,message):
    if not condition:raise RemoteError(grpc.StatusCode.DATA_LOSS,message)


def validate_projected_row(row, fields):
    fields=validate_fields(fields)
    present={field.name for field,_ in row.ListFields()}
    require(present==set(fields),'Projected metadata field presence mismatch')
    result={name:getattr(row,name) for name in fields}
    require(result['id']>=0,'Invalid projected metadata ID')
    if 'category' in result:require(bool(result['category']) and '\x00' not in result['category'],'Invalid projected category')
    if 'value' in result:require(math.isfinite(result['value']),'Nonfinite projected value')
    return result


def decode_projected_candidates(reply,lease,checked_ids,fields,vector_ids):
    fields=validate_fields(fields);checked_ids=list(checked_ids);vector_ids=list(vector_ids)
    require(reply.ByteSize()<=MAX_MESSAGE_BYTES,'Projected response exceeds message limit')
    require(reply.token==lease.token and reply.revision==lease.revision,'Projected lease mismatch')
    require(list(reply.checked_ids)==checked_ids and tuple(reply.fields)==fields,'Projected request coverage mismatch')
    rows=[validate_projected_row(row,fields) for row in reply.rows]
    matched=[row['id'] for row in rows];matched_set=set(matched)
    require(len(matched)==len(matched_set) and matched==[i for i in checked_ids if i in matched_set],
            'Projected matched ID coverage/order mismatch')
    expected=[i for i in vector_ids if i in matched_set]
    require(list(reply.vector_ids)==expected and reply.dimension==lease.dimension,'Projected vector coverage/dimension mismatch')
    require(len(reply.vectors_f32_le)==len(expected)*lease.dimension*4
            and len(reply.vectors_f32_le)<=VECTOR_PAYLOAD_BYTES,'Projected vector payload length mismatch')
    vectors=np.frombuffer(reply.vectors_f32_le,dtype='<f4').reshape(len(expected),lease.dimension)
    require(np.isfinite(vectors).all(),'Nonfinite projected vectors')
    return rows,expected,vectors
