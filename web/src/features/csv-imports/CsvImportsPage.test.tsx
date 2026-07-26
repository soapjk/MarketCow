import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { AuthContext } from "../auth/authContext";
import { CsvImportsPage } from "./CsvImportsPage";

test("uploads, preflights and creates an import with an explicit MIC", async () => {
  const requests: { url: string; init?: RequestInit }[] = [];
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    requests.push({ url, init });
    if (url.includes("/upload")) {
      return new Response(JSON.stringify({
        upload_id: "a".repeat(32),
        filename: "aapl.csv",
        byte_size: 42,
        sha256: "b".repeat(64),
        columns: ["DateTime", "Open", "High", "Low", "Close", "Volume"],
        delimiter: ",",
      }), { status: 201, headers: { "Content-Type": "application/json" } });
    }
    if (url.endsWith("/dry-run")) {
      return new Response(JSON.stringify({
        status: "valid", rows_valid: 1, rows_invalid: 0,
      }), { status: 200, headers: { "Content-Type": "application/json" } });
    }
    if (url.endsWith("/infer")) {
      return new Response(JSON.stringify({
        schema: "marketcow.csv-import-inference.v1",
        sample: {
          rows_seen: 100, timestamps_sampled: 100,
          timestamp_parse_failures: 0,
        },
        timezone: {
          value: "America/New_York", confidence: "high", score: 1,
          evidence: ["100% session alignment."], alternatives: [],
        },
        adjustment: {
          value: "raw", confidence: "low", score: 0.35,
          evidence: ["No adjustment metadata."],
        },
      }), { status: 200, headers: { "Content-Type": "application/json" } });
    }
    if (url.endsWith("/v1/admin/csv-imports") && init?.method === "POST") {
      return new Response(JSON.stringify({
        created: true, job: { job_id: "job-1", status: "queued" },
      }), { status: 200, headers: { "Content-Type": "application/json" } });
    }
    return new Response(JSON.stringify({ count: 0, items: [] }), {
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
        <CsvImportsPage />
      </AuthContext.Provider>
    </QueryClientProvider>,
  );

  expect(screen.getByLabelText("复权状态")).toHaveTextContent("前复权");
  expect(screen.getByLabelText("复权状态")).toHaveTextContent("后复权");
  expect(screen.getByLabelText("复权状态")).not.toHaveTextContent("adjusted");
  fireEvent.change(screen.getByLabelText("MIC"), { target: { value: "XNYS" } });
  expect(screen.getByText("AAPL.XNYS")).toBeInTheDocument();
  fireEvent.change(screen.getByLabelText("选择单个 CSV 文件"), {
    target: { files: [new File(["time,open\n"], "aapl.csv", { type: "text/csv" })] },
  });
  fireEvent.click(screen.getByRole("button", { name: "上传并预检" }));

  expect(await screen.findByText("valid")).toBeInTheDocument();
  expect(screen.getByLabelText("timestamp 列")).toHaveValue("DateTime");
  expect(screen.getByLabelText("open 列")).toHaveValue("Open");
  expect(screen.getByRole("region", { name: "自动语义分析" })).toHaveTextContent(
    "America/New_York",
  );
  expect(screen.getByRole("region", { name: "自动语义分析" })).toHaveTextContent(
    "low · 证据分 35",
  );
  expect(screen.getByRole("button", { name: "开始正式导入" })).toBeDisabled();
  fireEvent.click(screen.getByRole("checkbox", { name: /确认采用当前低置信度设置/ }));
  fireEvent.click(screen.getByRole("button", { name: "开始正式导入" }));
  await waitFor(() => expect(
    requests.some(({ url, init }) => (
      url.endsWith("/v1/admin/csv-imports") && init?.method === "POST"
    )),
  ).toBe(true));

  const dryRequest = requests.find(({ url }) => url.endsWith("/dry-run"));
  expect(JSON.parse(String(dryRequest?.init?.body))).toMatchObject({
    upload_id: "a".repeat(32),
    declaration: {
      instruments: { symbols: { "AAPL.US": "AAPL.XNYS" } },
      profile: { fixed_external_symbol: "AAPL.US" },
    },
  });
  vi.unstubAllGlobals();
});

test("preflights and imports every CSV in a folder with one shared format", async () => {
  const requests: { url: string; init?: RequestInit }[] = [];
  vi.stubGlobal("fetch", vi.fn(async (
    input: RequestInfo | URL,
    init?: RequestInit,
  ) => {
    const url = String(input);
    requests.push({ url, init });
    if (url.includes("/upload")) {
      const filename = new URL(url, "http://localhost").searchParams.get(
        "filename"
      ) ?? "";
      return new Response(JSON.stringify({
        upload_id: filename.startsWith("AAPL") ? "a".repeat(32) : "m".repeat(32),
        filename,
        byte_size: 42,
        sha256: filename.startsWith("AAPL") ? "b".repeat(64) : "c".repeat(64),
        columns: ["DateTime", "Open", "High", "Low", "Close", "Volume"],
        delimiter: ",",
      }), { status: 201, headers: { "Content-Type": "application/json" } });
    }
    if (url.endsWith("/infer")) {
      return new Response(JSON.stringify({
        schema: "marketcow.csv-import-inference.v1",
        sample: {
          rows_seen: 10, timestamps_sampled: 10,
          timestamp_parse_failures: 0,
        },
        timezone: {
          value: "America/New_York", confidence: "high", score: 1,
          evidence: ["Shared folder format."], alternatives: [],
        },
        adjustment: {
          value: "raw", confidence: "high", score: 1,
          evidence: ["Shared folder format."],
        },
      }), { status: 200, headers: { "Content-Type": "application/json" } });
    }
    if (url.endsWith("/dry-run")) {
      return new Response(JSON.stringify({
        status: "valid", rows_valid: 10, rows_invalid: 0,
      }), { status: 200, headers: { "Content-Type": "application/json" } });
    }
    if (url.endsWith("/v1/admin/csv-imports") && init?.method === "POST") {
      const body = JSON.parse(String(init.body));
      return new Response(JSON.stringify({
        created: true,
        job: {
          job_id: `job-${body.declaration.profile.fixed_external_symbol}`,
          status: "queued",
        },
      }), { status: 200, headers: { "Content-Type": "application/json" } });
    }
    return new Response(JSON.stringify({ count: 0, items: [] }), {
      status: 200, headers: { "Content-Type": "application/json" },
    });
  }));
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  render(
    <QueryClientProvider client={client}>
      <AuthContext.Provider value={{
        authenticated: true, actor: "admin", role: "admin",
        logout: () => undefined,
      }}>
        <CsvImportsPage />
      </AuthContext.Provider>
    </QueryClientProvider>,
  );
  const msft = new File(["csv"], "MSFT.US.csv", { type: "text/csv" });
  const aapl = new File(["csv"], "AAPL.US.csv", { type: "text/csv" });
  Object.defineProperty(msft, "webkitRelativePath", {
    value: "prices/MSFT.US.csv",
  });
  Object.defineProperty(aapl, "webkitRelativePath", {
    value: "prices/AAPL.US.csv",
  });

  fireEvent.change(screen.getByLabelText("选择 CSV 文件夹"), {
    target: { files: [msft, aapl] },
  });
  expect(screen.getByText(/AAPL\.US → AAPL\.XNAS/)).toBeInTheDocument();
  expect(screen.getByText(/MSFT\.US → MSFT\.XNAS/)).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "批量上传并预检" }));

  expect(await screen.findByText("2 / 2 通过")).toBeInTheDocument();
  expect(requests.filter(({ url }) => url.endsWith("/infer"))).toHaveLength(1);
  const uploads = requests.filter(({ url }) => url.includes("/upload"));
  expect(uploads.map(({ url }) => (
    new URL(url, "http://localhost").searchParams.get("filename")
  ))).toEqual(["AAPL.US.csv", "MSFT.US.csv"]);
  expect(requests
    .filter(({ url }) => (
      url.includes("/upload")
      || url.endsWith("/infer")
      || url.endsWith("/dry-run")
    ))
    .map(({ url }) => (
      url.includes("/upload")
        ? `upload:${
          new URL(url, "http://localhost").searchParams.get("filename")
        }`
        : url.endsWith("/infer") ? "infer" : "dry-run"
    ))).toEqual([
      "upload:AAPL.US.csv",
      "infer",
      "dry-run",
      "upload:MSFT.US.csv",
      "dry-run",
    ]);

  fireEvent.click(screen.getByRole("button", { name: "开始批量导入" }));
  await waitFor(() => expect(requests.filter(({ url, init }) => (
    url.endsWith("/v1/admin/csv-imports") && init?.method === "POST"
  ))).toHaveLength(2));
  const creates = requests
    .filter(({ url, init }) => (
      url.endsWith("/v1/admin/csv-imports") && init?.method === "POST"
    ))
    .map(({ init }) => JSON.parse(String(init?.body)));
  expect(creates.map((body) => body.declaration.instruments.symbols)).toEqual([
    { "AAPL.US": "AAPL.XNAS" },
    { "MSFT.US": "MSFT.XNAS" },
  ]);
  vi.unstubAllGlobals();
});

test("shows durable row progress, phase and a stale heartbeat warning", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => (
    new Response(JSON.stringify({
      count: 1,
      items: [{
        job_id: "job-live-progress",
        status: "running",
        phase: "importing",
        progress_percent: 50,
        rows_total: 10_000,
        rows_read: 5_000,
        rows_written: 5_000,
        heartbeat_at: "2026-01-01T00:00:00Z",
        updated_at: "2026-01-01T00:00:00Z",
      }],
    }), { status: 200, headers: { "Content-Type": "application/json" } })
  )));
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  render(
    <QueryClientProvider client={client}>
      <AuthContext.Provider value={{
        authenticated: true, actor: "admin", role: "admin",
        logout: () => undefined,
      }}>
        <CsvImportsPage />
      </AuthContext.Provider>
    </QueryClientProvider>,
  );

  expect(await screen.findByText("读取并写入")).toBeInTheDocument();
  expect(screen.getByText("5,000 / 10,000")).toBeInTheDocument();
  expect(screen.getByRole("progressbar")).toHaveValue(50);
  expect(screen.getByRole("alert")).toHaveTextContent("处理可能停滞");
  vi.unstubAllGlobals();
});
