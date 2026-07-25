import { ReconnectingEventStream } from "./eventStream";

test("parses events and closes the stream", () => {
  let source: Partial<EventSource> | undefined;
  const close = vi.fn();
  const onMessage = vi.fn();
  const stream = new ReconnectingEventStream({
    url: "/v1/admin/events",
    parse: JSON.parse,
    onMessage,
    eventSourceFactory: () => {
      source = { close, onopen: null, onmessage: null, onerror: null };
      return source as unknown as EventSource;
    },
  });

  stream.start();
  source?.onmessage?.call(source as EventSource, new MessageEvent("message", { data: '{"ok":true}' }));
  expect(onMessage).toHaveBeenCalledWith({ ok: true });
  stream.stop();
  expect(close).toHaveBeenCalled();
});

test("reconnect URL resumes after the last sequence", () => {
  vi.useFakeTimers();
  const urls: string[] = [];
  const sources: Partial<EventSource>[] = [];
  const stream = new ReconnectingEventStream({
    url: "/v1/admin/events?types=request.summary",
    parse: JSON.parse,
    onMessage: vi.fn(),
    eventSourceFactory: (url) => {
      urls.push(url);
      const source = { close: vi.fn(), onopen: null, onmessage: null, onerror: null };
      sources.push(source);
      return source as unknown as EventSource;
    },
  });
  stream.start();
  sources[0].onmessage?.call(
    sources[0] as unknown as EventSource,
    new MessageEvent("message", { data: '{"sequence":42}' }),
  );
  sources[0].onerror?.call(sources[0] as unknown as EventSource, new Event("error"));
  vi.advanceTimersByTime(1_000);
  expect(urls[1]).toContain("after_sequence=42");
  stream.stop();
  vi.useRealTimers();
});
