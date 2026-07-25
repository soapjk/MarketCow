import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import { OverviewPage } from "./OverviewPage";

test("renders the control-plane overview", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({
    schema: "marketcow.admin-overview.v1", generated_at: "2026-07-25T00:00:00Z",
    service: { status: "ok", version: "0.2.0", profile: "test" },
    storage: { ready: true }, providers: { total: 2, healthy: 1, items: [] },
    history_jobs: { items: [] },
  }), { status: 200, headers: { "Content-Type": "application/json" } })));
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(<QueryClientProvider client={client}><OverviewPage /></QueryClientProvider>);
  expect(await screen.findByText("1/2")).toBeInTheDocument();
  expect(screen.getByText("READY")).toBeInTheDocument();
  vi.unstubAllGlobals();
});
