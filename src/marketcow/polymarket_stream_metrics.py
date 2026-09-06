"""Constant-space interval metrics; never retain books or per-frame payloads."""
import json
import logging
import time
from datetime import datetime, timezone
from uuid import uuid4

LOGGER = logging.getLogger(__name__)
STAGES = ('receive_wait', 'decode', 'apply', 'encode_send',
          'replay_page', 'filter_convert', 'json_encode', 'socket_send', 'send_yield',
          'projection_lock_wait', 'recovery_validation', 'gap_maintenance',
          'book_validation', 'instrument_binding', 'subscriber_notify',
          'confirmation_apply')
DECODE_STAGES = ('worker_entry_wait', 'json_parse', 'model_validation',
                 'semantic_validation', 'hash_identity', 'worker_other', 'await_resume')


class StreamMetrics:
    def __init__(self, role):
        self.role = role
        self.connection_id = uuid4().hex
        self.started = time.monotonic()
        self.totals = {stage: [0, 0.0, 0.0] for stage in STAGES}
        self.decode_totals = {stage: [0, 0.0, 0.0] for stage in DECODE_STAGES}
        self.frame_count = 0
        self.last_frame = None

    def record_decode(self, timings, message):
        self.frame_count += 1
        self.last_frame = {'ordinal': self.frame_count, 'type': message.get('type'),
                           'cursor': message.get('cursor'),
                           'stream_instance_id': message.get('stream_instance_id')}
        for stage in DECODE_STAGES:
            values = self.decode_totals[stage]
            seconds = timings[stage]
            values[0] += 1
            values[1] += seconds
            values[2] = max(values[2], seconds)

    def record(self, stage, seconds):
        values = self.totals[stage]
        values[0] += 1
        values[1] += seconds
        values[2] = max(values[2], seconds)

    def emit(self, cursor, *, force=False, error=None, scale=None):
        now = time.monotonic()
        if not force and now - self.started < 5:
            return
        LOGGER.warning('polymarket_stream_stages %s', json.dumps({
            'at': datetime.now(timezone.utc).isoformat(), 'role': self.role,
            'connection_id': self.connection_id, 'local_cursor': cursor,
            'interval_seconds': now-self.started, 'stages_count_sum_max_seconds': self.totals,
            'receive_wait_is_network_rtt': False, 'queue_occupancy': 'not_measured',
            'error': error,
            'projection_scale': scale,
            'stage_measurement_boundary': 'wall time including scheduling and lock waits; not pure CPU; stages are not necessarily additive',
            'decode_detail_count_sum_max_seconds': self.decode_totals,
            'last_frame': self.last_frame,
            'decode_measurement_boundary': 'successful frames only; wall time including scheduling/GIL; entry wait and await resume are not pure queue or GIL measurements; model validators may include conversion',
        }, separators=(',', ':')))
        self.started = now
        self.totals = {stage: [0, 0.0, 0.0] for stage in STAGES}
        self.decode_totals = {stage: [0, 0.0, 0.0] for stage in DECODE_STAGES}
