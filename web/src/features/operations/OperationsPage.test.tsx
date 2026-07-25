import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { AuthContext } from "../auth/authContext";
import { OperationsPage } from "./OperationsPage";

test("submits an explicit date range and provider-specific interval", async () => {
  let submitted: Record<string, unknown> | undefined;
  vi.stubGlobal("fetch", vi.fn(async (url: RequestInfo | URL, init?: RequestInit) => {
    if (init?.method === "POST") {
      submitted = JSON.parse(String(init.body));
      return new Response(JSON.stringify({
        job_id: "job-1", created: true, job: { job_id: "job-1" },
      }), { status: 202, headers: { "Content-Type": "application/json" } });
    }
    const body = String(url).includes("/providers")
      ? { items: [], page: { total: 0 } }
      : { items: [], page: { total: 0 } };
    return new Response(JSON.stringify(body), {
      status: 200, headers: { "Content-Type": "application/json" },
    });
  }));
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  render(
    <QueryClientProvider client={client}>
      <AuthContext.Provider value={{
        authenticated: true, actor: "admin", role: "admin", logout: () => undefined,
      }}>
        <OperationsPage />
      </AuthContext.Provider>
    </QueryClientProvider>,
  );

  const provider = screen.getByLabelText("Provider");
  const interval = screen.getByLabelText("周期");
  fireEvent.change(provider, { target: { value: "tushare" } });
  expect(screen.queryByRole("option", { name: "日线" })).not.toBeInTheDocument();
  fireEvent.change(interval, { target: { value: "5m" } });
  fireEvent.change(screen.getByLabelText("开始日期"), {
    target: { value: "2026-06-01" },
  });
  fireEvent.change(screen.getByLabelText("结束日期"), {
    target: { value: "2026-06-30" },
  });
  fireEvent.click(screen.getByRole("button", { name: "创建任务" }));

  await waitFor(() => expect(submitted).toBeDefined());
  expect(submitted).toMatchObject({
    provider: "tushare",
    range: "custom",
    range_start: "2026-06-01T00:00:00.000Z",
    range_end: "2026-06-30T23:59:59.999Z",
    interval: "5m",
    adjustment: "raw",
  });
  vi.unstubAllGlobals();
});
