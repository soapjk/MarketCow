import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import { App } from "./App";

test("renders the administration shell and overview route", async () => {
  window.location.hash = "#/overview";
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(<QueryClientProvider client={client}><App /></QueryClientProvider>);
  expect(await screen.findByRole("heading", { name: "总览", level: 1 })).toBeInTheDocument();
  expect(screen.getByRole("navigation", { name: "主导航" })).toBeInTheDocument();
  expect(screen.getByText("正在汇总系统状态…")).toBeInTheDocument();
});

test("exposes the CSV import page in the main navigation", async () => {
  window.location.hash = "#/csv-imports";
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(<QueryClientProvider client={client}><App /></QueryClientProvider>);
  expect(await screen.findByRole("heading", { name: "CSV 导入", level: 1 })).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "上传历史 K 线" })).toBeInTheDocument();
});
