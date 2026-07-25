import type { AdminEvent, RequestSummaryPayload } from "./types";

export class RollingEventBuffer {
  private values: AdminEvent<RequestSummaryPayload>[] = [];

  constructor(readonly capacity = 2_000) {
    if (!Number.isInteger(capacity) || capacity < 1 || capacity > 100_000) {
      throw new Error("event buffer capacity is invalid");
    }
  }

  append(events: AdminEvent<RequestSummaryPayload>[]) {
    if (!events.length) return;
    this.values.push(...events);
    if (this.values.length > this.capacity) {
      this.values.splice(0, this.values.length - this.capacity);
    }
  }

  snapshot() {
    return this.values.slice();
  }

  clear() {
    this.values.length = 0;
  }
}

export type LiveSnapshot = {
  timeline: { at: number; requests: number; p50: number; p95: number }[];
  statuses: { name: string; value: number }[];
  inFlight: number;
  errors: AdminEvent<RequestSummaryPayload>[];
  total: number;
};

function percentile(values: number[], quantile: number) {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * quantile))];
}

export function summarizeRequests(
  events: AdminEvent<RequestSummaryPayload>[],
  now = Date.now(),
  windowSeconds = 60,
): LiveSnapshot {
  const start = now - windowSeconds * 1_000;
  const buckets = new Map<number, AdminEvent<RequestSummaryPayload>[]>();
  const statuses = new Map<string, number>();
  const selected = events.filter((event) => {
    const at = Date.parse(event.occurred_at);
    return Number.isFinite(at) && at >= start && at <= now + 1_000;
  });
  for (const event of selected) {
    const at = Math.floor(Date.parse(event.occurred_at) / 1_000) * 1_000;
    const bucket = buckets.get(at) ?? [];
    bucket.push(event);
    buckets.set(at, bucket);
    const status = event.payload.status_family || "unknown";
    statuses.set(status, (statuses.get(status) ?? 0) + 1);
  }
  const timeline = [];
  for (let index = windowSeconds - 1; index >= 0; index--) {
    const at = Math.floor((now - index * 1_000) / 1_000) * 1_000;
    const bucket = buckets.get(at) ?? [];
    const durations = bucket.map((event) => Number(event.payload.duration_ms) || 0);
    timeline.push({
      at,
      requests: bucket.length,
      p50: percentile(durations, 0.5),
      p95: percentile(durations, 0.95),
    });
  }
  return {
    timeline,
    statuses: [...statuses].sort().map(([name, value]) => ({ name, value })),
    inFlight: selected.at(-1)?.payload.in_flight ?? 0,
    errors: selected.filter((event) =>
      event.payload.status_family === "5xx" || Boolean(event.payload.exception)
    ).slice(-20).reverse(),
    total: selected.length,
  };
}
