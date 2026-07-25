import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
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
