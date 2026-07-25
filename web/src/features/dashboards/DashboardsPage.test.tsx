import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import { DashboardsPage } from "./DashboardsPage";

test("renders multiple projects from the server registry", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({
    schema: "marketcow.dashboard-registry.v1",
    items: [
      { key: "market", project: "MarketCow", name: "库存", description: "",
        dashboard_uid: "market", panel_id: null, theme: "current", sort_order: 1,
        path: "/d/market/market?orgId=1" },
      { key: "api", project: "API Service", name: "访问", description: "",
        dashboard_uid: "api", panel_id: 8, theme: "dark", sort_order: 2,
        path: "/d-solo/api/api?panelId=8" },
    ],
  }), { status: 200, headers: { "Content-Type": "application/json" } })));
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(<QueryClientProvider client={client}><DashboardsPage /></QueryClientProvider>);
  expect(await screen.findByTitle("MarketCow - 库存")).toBeInTheDocument();
  expect(screen.getByTitle("API Service - 访问")).toBeInTheDocument();
  vi.unstubAllGlobals();
});
