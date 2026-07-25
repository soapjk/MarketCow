export type RequestSummaryPayload = {
  method: string;
  route: string;
  status_family: string;
  duration_ms: number;
  in_flight: number;
  exception: string;
};

export type AdminEvent<T = Record<string, unknown>> = {
  schema_version: "marketcow.admin-event.v1";
  event_id: string;
  type: "request.summary" | "history_job.updated" | "provider.status" | "stream.heartbeat" | "stream.gap";
  source: string;
  occurred_at: string;
  sequence: number;
  payload: T;
};

export type MarketCandle = {
  at: number;
  open: number;
  close: number;
  low: number;
  high: number;
  volume: number;
};

export type OrderBookLevel = { price: number; size: number };
export type OrderBookSnapshot = {
  at: number;
  bids: OrderBookLevel[];
  asks: OrderBookLevel[];
};
