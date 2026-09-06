"""Strict token-scoped recovery contract; never implies global readiness."""
from typing import Any


def is_authoritative_book_snapshot(event: Any) -> bool:
    """Recognize a bound full book from WS or the exact CLOB /books shape.

    REST recovery deliberately preserves the upstream payload and therefore
    does not invent the WS-only ``event_type`` field.  Identity and all facts
    required to construct a full book remain mandatory; an incremental or
    malformed payload can never satisfy a recovery completion.
    """
    if (
        event.event_type != 'book' or not event.applied
        or not event.token_id or not event.condition_id
    ):
        return False
    raw = event.raw_payload
    if not isinstance(raw, dict):
        return False
    source_type = raw.get('event_type')
    if source_type not in (None, 'book'):
        return False
    if raw.get('asset_id') != event.token_id or raw.get('market') != event.condition_id:
        return False
    full_book = (
        isinstance(raw.get('timestamp'), str) and bool(raw['timestamp'])
        and isinstance(raw.get('hash'), str) and bool(raw['hash'])
        and isinstance(raw.get('bids'), list)
        and isinstance(raw.get('asks'), list)
    )
    # Official WS book frames do not carry tick_size; their normalized event
    # is already bound to the verified catalog tick. Exact REST /books payloads
    # have no event_type, so their explicit tick remains mandatory.
    return full_book and (
        source_type == 'book'
        or (isinstance(raw.get('tick_size'), str) and bool(raw['tick_size']))
    )


def validate_token_recovery(event: Any) -> str | None:
    """Return a bound token for explicitly scoped events, otherwise legacy None.

    Token recovery is additive to live.v2 recovery events. Unknown explicit
    scopes fail closed; absence retains the existing generation recovery path.
    """
    payload = event.canonical_payload
    if 'recovery_scope' not in payload:
        return None
    if payload['recovery_scope'] != 'token':
        raise ValueError('unsupported recovery scope')
    if event.event_type not in {'recovery_started', 'recovery_completed'}:
        raise ValueError('token recovery requires recovery event')
    if not event.applied or not event.token_id or not event.market_id or not event.condition_id:
        raise ValueError('token recovery requires applied bound identity')
    if payload['token_id'] != event.token_id:
        raise ValueError('token recovery identity differs')
    if not isinstance(payload['recovery_id'], str) or not payload['recovery_id']:
        raise ValueError('token recovery id required')
    if event.event_type == 'recovery_started':
        if len(event.gaps) != 1 or event.gaps[0].token_id != event.token_id or event.gaps[0].resolved:
            raise ValueError('token recovery must invalidate exactly its own token')
        if not isinstance(payload['reason'], str) or not payload['reason']:
            raise ValueError('token recovery reason required')
    else:
        if event.gaps or payload['resolved_gap_token_ids'] != [event.token_id]:
            raise ValueError('token recovery may resolve only its own token')
        if not isinstance(payload['snapshot_cursor'], int) or isinstance(payload['snapshot_cursor'], bool):
            raise ValueError('snapshot cursor required')
        if not 0 < payload['snapshot_cursor'] < event.cursor:
            raise ValueError('recovery completion requires prior snapshot')
    return event.token_id
