# Official history probe r1: local-only diagnosis

No network request was made during this follow-up diagnosis. Production services,
proxy configuration, scopes and accounts were unchanged.

## Verified saved evidence

- Evidence directory: `/private/tmp/marketcow-official-history-r1`.
- Raw response: `gamma-1088482.raw.json`, 17 bytes, exactly
  `error code: 1010\n`; SHA-256
  `2938e9f1284180959e33ab1718d0793a72ff6e4cdb8108c34dcd14e69446de5c`.
- Original `report.json` SHA-256:
  `a76829164b19d97e1baf81052f2076cc9c14d6bc417371c55fcfbe69fff2e51e`.
- GET `https://gamma-api.polymarket.com/markets/1088482` returned HTTP 403.
  Request began 2026-09-08T12:14:28.566890Z; response read completed
  12:14:29.787730Z. Response identifies Cloudflare, ray
  `a37ddf8bda3ad873-MCI`, and `cfEdge;dur=4,cfOrigin;dur=0`.
  These are server-reported headers, not an independently measured route or
  definitive evidence of a particular blocking rule.
- No CLOB market or price-history request followed. This is not an empty history
  result and says nothing about whether historical prices can be paired to labels.

## Request construction and environment

`scripts/probe_official_history.py` uses a GET with `Accept-Encoding: identity`,
urllib's default User-Agent, no authorization, disabled redirects, no retries,
and an explicit `ProxyHandler({})`.

The current interpreter is Python 3.13.11; its default opener User-Agent is
`Python-urllib/3.13`. Current proxy environment variables exist (both upper and
lowercase HTTP, HTTPS, ALL and NO_PROXY variants). Their values are not printed
or stored here. Offline opener inspection confirms the explicit empty proxy
handler suppresses environment proxy handling, whereas a default opener installs
it. This does not prove the public egress IP: transparent routing is not measured.
Current configuration also does not establish configuration at the earlier times.

The prior successful Gamma reads for 2587796, 2587798 and 3013350 used the same
documented `/markets/{id}` route shape, different IDs and earlier timestamps.
Their full request script, request headers and contemporaneous egress evidence
were not retained in the available evidence. A bounded repository search found
no saved successful-request script. Prior success is not a controlled comparison
to this request; neither User-Agent nor proxy differences are established causes.

## Findings and next boundary

The HTTP refusal occurred before JSON parsing, identity validation, end-date
selection or historical-price processing. Those stages cannot explain this 403.
No malformed URL/method has been identified. An edge rejection is observed;
the exact rule and whether request characteristics or route conditions caused it
remain unknown. Do not label it a geographic or authentication block without
additional evidence, and do not change routes/headers to circumvent it.

The original report retained response headers including a server cookie; it is
kept locally unchanged, not forwarded. The script was subsequently changed to a
safe header allowlist. That post-run change cannot explain the earlier 403.

No transport fix or API rerun is justified by this comparison alone. Required
missing evidence is the earlier request construction and route configuration,
or an operator/source explanation of the captured refusal. The next permitted
probe must use an explicitly allowed configuration and a new finite budget;
this diagnosis does not authorize a network change or retry.

## Follow-up correction

Cloudflare's official error-1010 documentation identifies client/browser-signature
blocking and directs visitors to the website owner; this is more specific than
the initial edge-only diagnosis. It does not identify the exact matching rule or
prove that no other restrictions apply. Reference:
https://developers.cloudflare.com/support/troubleshooting/http-status-codes/cloudflare-1xxx-errors/error-1010/

The old probe's endDate-minus-seven-days logic could request future time for an
unresolved championship market. It never executed in r1 because the first GET
failed. The corrected probe requires explicit past UTC-second start/end values,
at most seven days, validates before network/output creation and again before
each history request, and records its executing source SHA. Market 1088482 is
labelled a championship candidate based on peer evidence, not independently
verified single-match or paired-label data. Seven offline window tests pass.
No additional Polymarket request was made. The earlier report is not rewritten
or retroactively assigned the new source hash.

Concrete external dependency: the website operator must clarify the captured
1010 refusal / supported automated-client access (URL, UTC and CF-RAY above).
No contact was sent, no network configuration changed, and no browser identity
was spoofed. Source review and offline tests do not establish allowed access.

## Completed offline follow-up (13 tests)

Command: `python3 -m pytest tests/test_official_history_probe.py -q`.
Result: 13 passed; Ruff passed for both script and test module.

Script SHA-256:
`e14ca11805ea1c31633c42b046b0ccb826fe46b6644feecad221e60586952340`.
Test SHA-256:
`fd41e2a1ba535239c3accdfd22c1eb5db273c7b34f0076227a9daa4023190c2d`.

Coverage: future end, missing timezone, zero/reversed/oversized window; wrong
Gamma market identity; mismatched CLOB condition/token and duplicate token;
single-match research declaration rejected for this championship candidate;
synthetic HTTP 403 stops after the first request and retains raw error evidence.
The mocked opener makes no network calls. The report's executing code SHA is
asserted against the actual file bytes in the test.

CLI now also requires `--research-type availability_probe`. Configuration records
window, fidelity, research type, output, proxy/redirect policy and default-UA
policy; the existing fixed request/byte/time budgets remain explicit in report.
No actual probe execution was performed for this source version. Original r1
evidence stays unchanged. Championship classification remains peer-supplied, not
independently verified rules or an eligible single-match dataset.

## Budget follow-up (20 offline tests)

Removed the minimum 0.01-second timeout fallback. Each request now checks total
remaining monotonic budget before recording/constructing the request, then checks
again immediately before `open`; nonpositive time stops without another open.
Wall time below the capture-start clock also stops requests. This is not a
persisted clock high-watermark or a hard-real-time guarantee: an already blocked
library call, scheduler delay, and output finalization can exceed wall deadlines.

Added tests for expiry before the first request, expiry immediately before open,
expiry after the first response preventing the second request, end-to-end wall
clock rollback, and body sizes 1MiB-1, 1MiB and 1MiB+1. No overflow probe byte is
read. At the exact byte budget we conservatively report `raw_complete=false` and
`byte_budget_eof_unverified`; exact-size completeness is intentionally unknown.
Byte accounting covers application response-body reads, not TLS/header traffic
or the HTTP library's internal buffers.

Command: `python3 -m pytest tests/test_official_history_probe.py -q`:
20 passed in 0.07s. Ruff passed. Script SHA:
`9ad1dcd8d86256479a2fed40920fdd256e17abea15614c0437e3080b07468e30`.
Test SHA:
`378a46bd81c07b02ffe0e1766eb2c280026203affb55a1d9ac8e66bdb6fbefc1`.
Zero official API requests; prior evidence is unchanged.

## Same-URL successful lifecycle evidence

The U1 lifecycle observation `e98acc85124391b4387e5cbc5a3bdf126c6ecaebc9fbbd2e0a62c83c8eb2f4c2.json`
is a stronger comparison than the earlier three different market GETs. Its
4948-byte file hash and embedded raw-response hash
`f771f7c06028cdc56ab3c2148104c4dca86b21c742b07047a6f78c743ecc4c1f`
were independently checked. It records the exact same Gamma market URL and
capture time 2026-09-06T04:31:09.078599322Z. The envelope includes catalog revision
and evidence, but no executable SHA, process identity, effective headers, proxy
route or egress identity. Thus it proves a saved successful source response,
not the exact historical client environment.

Current Rust `source_lifecycle.rs::refresh` uses client GET, error_for_status,
bounded body collection, parsing/validation and raw hash retention. Both current
client builders examined only set timeout, with no explicit UA/proxy override.
Local Cargo.lock pins reqwest 0.12.28; its inspected builder source defaults to
system proxy discovery and Accept */*. The Python probe explicitly suppresses
environment proxies. These are code differences, not a controlled network test.

The U1 bounded-v1 previous collector unit (2063 bytes, SHA
`65ca7f344fa44d26eab424a518a01f603a18b99bfc140dcd7a9711f276d19e18`)
contains HTTP_PROXY and HTTPS_PROXY assignments in its launch command; values
were not exposed. That archived copy was saved at September 6 16:41 +0800,
after the observation at 12:31 +0800. The bounded-v1 binary's displayed mtime
was also later, 16:39. Neither timestamps nor current source prove that release
or unit was in force when the observation was captured. Exact historical binary
and effective environment remain unbound. No API rerun or proxy change was made.
