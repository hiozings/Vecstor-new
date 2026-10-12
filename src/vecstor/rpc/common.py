"""Shared wire limits and explicit RPC failure semantics."""

import math

import grpc

MAX_MESSAGE_BYTES = 4 * 1024 * 1024
VECTOR_PAYLOAD_BYTES = 1024 * 1024
MAX_BATCH_ROWS = 1024
DEFAULT_BATCH_ROWS = 256
OPTIONS = [("grpc.max_receive_message_length", MAX_MESSAGE_BYTES),
           ("grpc.max_send_message_length", MAX_MESSAGE_BYTES),
           ("grpc.enable_retries", 0)]


class RemoteError(ValueError):
    def __init__(self, code, details):
        self.code = code
        self.details = details
        super().__init__(f"{code.name}: {details}")


def positive_timeout(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError("timeout must be a finite positive number")
    return float(value)


def active(context):
    if not context.is_active():
        context.abort(grpc.StatusCode.CANCELLED, "Request cancelled")
