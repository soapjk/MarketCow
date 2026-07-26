import {
  ApiError,
  AUTHENTICATION_REQUIRED_EVENT,
  createApiClient,
} from "./api";

test("returns JSON and sends same-origin credentials", async () => {
  const fetchImpl = vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) => {
    expect(init?.credentials).toBe("same-origin");
    return new Response(JSON.stringify({ status: "ok" }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  });
  const client = createApiClient({ baseUrl: "http://local", fetchImpl });
  await expect(client.request("/v1/health")).resolves.toEqual({ status: "ok" });
});

test("normalizes structured API errors", async () => {
  const client = createApiClient({
    fetchImpl: async () => new Response(JSON.stringify({ detail: "not ready" }), {
      status: 503,
      headers: { "Content-Type": "application/json", "x-request-id": "req-1" },
    }),
  });
  await expect(client.request("/v1/readiness")).rejects.toEqual(
    new ApiError("not ready", 503, "req-1"),
  );
});

test("normalizes authentication errors and announces an expired session", async () => {
  const listener = vi.fn();
  window.addEventListener(AUTHENTICATION_REQUIRED_EVENT, listener);
  const client = createApiClient({
    fetchImpl: async () => new Response(JSON.stringify({
      detail: { code: "authentication_required" },
    }), {
      status: 401,
      headers: { "Content-Type": "application/json" },
    }),
  });

  await expect(client.request("/v1/admin/csv-imports")).rejects.toEqual(
    new ApiError("登录会话已失效，请重新登录", 401),
  );
  expect(listener).toHaveBeenCalledOnce();
  window.removeEventListener(AUTHENTICATION_REQUIRED_EVENT, listener);
});

test("adds CSRF and request IDs only to mutations", async () => {
  document.cookie = "marketcow_csrf=csrf-value";
  const fetchImpl = vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) => {
    const headers = new Headers(init?.headers);
    expect(headers.get("X-CSRF-Token")).toBe("csrf-value");
    expect(headers.get("X-Request-ID")).toBeTruthy();
    return new Response(null, { status: 204 });
  });
  const client = createApiClient({ fetchImpl });
  await client.request("/v1/admin/action", { method: "POST" });
});
