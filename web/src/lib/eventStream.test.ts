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
      return source as EventSource;
    },
  });

  stream.start();
  source?.onmessage?.call(source as EventSource, new MessageEvent("message", { data: '{"ok":true}' }));
  expect(onMessage).toHaveBeenCalledWith({ ok: true });
  stream.stop();
  expect(close).toHaveBeenCalled();
});
