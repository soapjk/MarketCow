import { useEffect, useRef, useState } from "react";
import { ReconnectingEventStream, type ConnectionState } from "../../lib/eventStream";
import { RollingEventBuffer, summarizeRequests, type LiveSnapshot } from "./eventBuffer";
import type { AdminEvent, RequestSummaryPayload } from "./types";

const empty = summarizeRequests([], Date.now());

export function useLiveRequests(paused: boolean) {
  const buffer = useRef(new RollingEventBuffer());
  const pending = useRef<AdminEvent<RequestSummaryPayload>[]>([]);
  const [state, setState] = useState<ConnectionState>("idle");
  const [snapshot, setSnapshot] = useState<LiveSnapshot>(empty);

  useEffect(() => {
    if (paused) {
      setState("idle");
      return;
    }
    const stream = new ReconnectingEventStream<AdminEvent<RequestSummaryPayload>>({
      url: "/v1/admin/events?types=request.summary",
      parse: JSON.parse,
      onState: setState,
      onMessage: (event) => {
        if (event.type === "request.summary") pending.current.push(event);
      },
    });
    stream.start();
    const flush = window.setInterval(() => {
      if (pending.current.length) {
        buffer.current.append(pending.current.splice(0));
      }
      setSnapshot(summarizeRequests(buffer.current.snapshot(), Date.now()));
    }, 250);
    return () => {
      window.clearInterval(flush);
      stream.stop();
    };
  }, [paused]);

  return {
    state,
    snapshot,
    clear: () => {
      pending.current.length = 0;
      buffer.current.clear();
      setSnapshot(summarizeRequests([], Date.now()));
    },
  };
}
