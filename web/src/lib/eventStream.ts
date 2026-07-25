export type ConnectionState = "idle" | "connecting" | "open" | "retrying" | "closed";

export type StreamOptions<T> = {
  url: string;
  parse: (data: string) => T;
  onMessage: (event: T) => void;
  onState?: (state: ConnectionState) => void;
  eventSourceFactory?: (url: string) => EventSource;
  maxBackoffMs?: number;
};

export class ReconnectingEventStream<T> {
  private source?: EventSource;
  private retry?: number;
  private attempt = 0;
  private stopped = true;
  private lastSequence = 0;

  constructor(private readonly options: StreamOptions<T>) {}

  start() {
    if (!this.stopped) return;
    this.stopped = false;
    this.connect();
  }

  stop() {
    this.stopped = true;
    if (this.retry) window.clearTimeout(this.retry);
    this.source?.close();
    this.options.onState?.("closed");
  }

  private connect() {
    if (this.stopped) return;
    this.options.onState?.(this.attempt ? "retrying" : "connecting");
    const factory = this.options.eventSourceFactory ?? ((url) => new EventSource(url, { withCredentials: true }));
    const separator = this.options.url.includes("?") ? "&" : "?";
    const url = this.lastSequence
      ? `${this.options.url}${separator}after_sequence=${this.lastSequence}`
      : this.options.url;
    this.source = factory(url);
    this.source.onopen = () => {
      this.attempt = 0;
      this.options.onState?.("open");
    };
    this.source.onmessage = (message) => {
      try {
        const event = this.options.parse(message.data);
        if (event && typeof event === "object" && "sequence" in event) {
          const sequence = Number((event as { sequence?: unknown }).sequence);
          if (Number.isInteger(sequence) && sequence > this.lastSequence) {
            this.lastSequence = sequence;
          }
        }
        this.options.onMessage(event);
      } catch {
        // Malformed events are ignored; protocol-level reporting is added with MCHR-49.
      }
    };
    this.source.onerror = () => {
      this.source?.close();
      if (this.stopped) return;
      const delay = Math.min(1_000 * 2 ** this.attempt++, this.options.maxBackoffMs ?? 30_000);
      this.retry = window.setTimeout(() => this.connect(), delay);
    };
  }
}
