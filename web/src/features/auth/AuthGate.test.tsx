import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen } from "@testing-library/react";
import { AUTHENTICATION_REQUIRED_EVENT } from "../../lib/api";
import { AuthGate } from "./AuthGate";

test("renders username and password fields for an unauthenticated browser", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({
    detail: { code: "authentication_required" },
  }), {
    status: 401,
    headers: { "Content-Type": "application/json" },
  })));
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <AuthGate><div>authenticated content</div></AuthGate>
    </QueryClientProvider>,
  );
  expect(await screen.findByLabelText("用户名")).toBeInTheDocument();
  expect(screen.getByLabelText("密码")).toBeInTheDocument();
  expect(screen.queryByText("访问令牌")).not.toBeInTheDocument();
  vi.unstubAllGlobals();
});

test("revalidates a cached session when an admin request reports 401", async () => {
  const fetchMock = vi.fn()
    .mockResolvedValueOnce(new Response(JSON.stringify({
      authenticated: true, actor: "admin", role: "admin",
    }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    }))
    .mockResolvedValue(new Response(JSON.stringify({
      detail: { code: "authentication_required" },
    }), {
      status: 401,
      headers: { "Content-Type": "application/json" },
    }));
  vi.stubGlobal("fetch", fetchMock);
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <AuthGate><div>authenticated content</div></AuthGate>
    </QueryClientProvider>,
  );
  expect(await screen.findByText("authenticated content")).toBeInTheDocument();

  act(() => {
    window.dispatchEvent(new Event(AUTHENTICATION_REQUIRED_EVENT));
  });

  expect(await screen.findByLabelText("用户名")).toBeInTheDocument();
  expect(fetchMock).toHaveBeenCalledTimes(2);
  vi.unstubAllGlobals();
});
