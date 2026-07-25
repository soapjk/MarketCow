import { RollingEventBuffer, summarizeRequests } from "./eventBuffer";
import type { AdminEvent, RequestSummaryPayload } from "./types";

function event(index: number, overrides: Partial<RequestSummaryPayload> = {}): AdminEvent<RequestSummaryPayload> {
  return {
    schema_version: "marketcow.admin-event.v1",
    event_id: String(index),
    type: "request.summary",
    source: "test",
    occurred_at: new Date(1_000_000 + index * 100).toISOString(),
    sequence: index,
    payload: {
      method: "GET", route: "/v1/health", status_family: "2xx",
      duration_ms: index, in_flight: index % 3, exception: "", ...overrides,
    },
  };
}

test("rolling buffer remains bounded during a long stream", () => {
  const buffer = new RollingEventBuffer(2_000);
  for (let index = 0; index < 20_000; index += 100) {
    buffer.append(Array.from({ length: 100 }, (_, offset) => event(index + offset)));
  }
  expect(buffer.snapshot()).toHaveLength(2_000);
  expect(buffer.snapshot()[0].sequence).toBe(18_000);
});

test("summarizes fixed-window rates latency statuses and errors", () => {
  const events = [
    event(1), event(2, { duration_ms: 100 }),
    event(3, { status_family: "5xx", exception: "ValueError" }),
  ];
  const snapshot = summarizeRequests(events, 1_001_000, 2);
  expect(snapshot.total).toBe(3);
  expect(snapshot.statuses).toEqual([{ name: "2xx", value: 2 }, { name: "5xx", value: 1 }]);
  expect(snapshot.errors).toHaveLength(1);
  expect(Math.max(...snapshot.timeline.map((item) => item.p95))).toBeGreaterThan(0);
});
