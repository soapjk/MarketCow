"""Constant-space interval metrics; never retain books or per-frame payloads."""
import json
import logging
import time
import threading
from collections import deque
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

_LOCAL = threading.local()
SYNC_STAGES = frozenset(('replay_page', 'filter_convert', 'json_encode'))


class SlowTimeline:
    """Calling-thread-only spans; fixed memory, no payload retention."""
    def __init__(self):
        self.spans = deque(maxlen=1024)
        self.evicted = 0

    def add(self, stage, connection, start, end):
        if len(self.spans) == self.spans.maxlen:
            self.evicted += 1
        self.spans.append((start, end, stage, connection))

    def overlap(self, start, end):
        totals = dict.fromkeys(SYNC_STAGES, 0.0)
        union_end = start
        union = 0.0
        for a, b, stage, _ in sorted(self.spans):
            lo, hi = max(start, a), min(end, b)
            if hi > lo:
                totals[stage] += hi-lo
                union += max(0.0, hi-max(lo, union_end))
                union_end = max(union_end, hi)
        return {'overlap_seconds': totals, 'union_seconds': union,
                'unexplained_seconds': max(0.0, end-start-union),
                'coverage_incomplete': bool(self.evicted and self.spans and start < self.spans[0][0]),
                'spans_evicted_total': self.evicted}


def timeline():
    if not hasattr(_LOCAL, 'timeline'):
        _LOCAL.timeline = SlowTimeline()
    return _LOCAL.timeline


class StreamMetrics:
    def __init__(self, role):
        self.role = role
        self.connection_id = uuid4().hex
        self.started = time.monotonic()
        self.totals = {stage: [0, 0.0, 0.0] for stage in STAGES}
        self.decode_totals = {stage: [0, 0.0, 0.0] for stage in DECODE_STAGES}
        self.frame_count = 0
        self.last_frame = None
        self.slow_frames = []
        self.slow_seen = 0

    def record_decode(self, timings, message):
        self.frame_count += 1
        self.last_frame = {'ordinal': self.frame_count, 'type': message.get('type'),
                           'cursor': message.get('cursor'),
                           'stream_instance_id': message.get('stream_instance_id')}
        trace = timings.get('_trace')
        if trace and 'resume' in trace and trace['resume']-trace['finish'] >= .001:
            self.slow_seen += 1
            if len(self.slow_frames) < 8:
                self.slow_frames.append({**self.last_frame, **trace,
                    **timeline().overlap(trace['finish'], trace['resume'])})
        for stage in DECODE_STAGES:
            values = self.decode_totals[stage]
            seconds = timings[stage]
            values[0] += 1
            values[1] += seconds
            values[2] = max(values[2], seconds)

    def record(self, stage, seconds):
        if stage in SYNC_STAGES:
            end = time.perf_counter()
            # Existing callers measure synchronous sections immediately before
            # record(); reconstructed start has small instrumentation boundary skew.
            timeline().add(stage, self.connection_id, end-seconds, end)
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
            'slow_decode_frames': self.slow_frames,
            'slow_decode_seen': self.slow_seen,
            'slow_decode_omitted': max(0, self.slow_seen-len(self.slow_frames)),
            'timeline_scope': 'calling thread synchronous replay/filter/json only; start reconstructed from existing duration; not full loop, CPU, GIL or whole executor',
            'decode_measurement_boundary': 'successful frames only; wall time including scheduling/GIL; entry wait and await resume are not pure queue or GIL measurements; model validators may include conversion',
        }, separators=(',', ':')))
        self.started = now
        self.totals = {stage: [0, 0.0, 0.0] for stage in STAGES}
        self.decode_totals = {stage: [0, 0.0, 0.0] for stage in DECODE_STAGES}
        self.slow_frames = []
        self.slow_seen = 0
