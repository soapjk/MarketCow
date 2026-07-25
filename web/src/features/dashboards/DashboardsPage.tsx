import { useQuery } from "@tanstack/react-query";
import { api } from "../../lib/api";
import { GrafanaFrame } from "./GrafanaFrame";
import type { DashboardRegistryDocument } from "./types";

export function DashboardsPage() {
  const query = useQuery({
    queryKey: ["dashboard-registry"],
    queryFn: () => api.request<DashboardRegistryDocument>("/v1/admin/dashboards"),
  });

  if (query.isPending) {
    return <section className="page-state" aria-live="polite">正在读取看板注册表…</section>;
  }
  if (query.isError) {
    return (
      <section className="page-state error-state">
        <strong>无法读取看板注册表</strong>
        <span>{query.error.message}</span>
        <button type="button" onClick={() => query.refetch()}>重试</button>
      </section>
    );
  }
  if (!query.data.items.length) {
    return <section className="page-state">当前没有启用的 Grafana 看板。</section>;
  }

  return (
    <section className="dashboard-page">
      <div className="page-intro">
        <div>
          <p className="eyebrow">REGISTERED / READ-ONLY</p>
          <h2>统一数据看板</h2>
        </div>
        <p>看板由本机 Grafana 实例渲染；管理操作仍由 MarketCow 控制面处理。</p>
      </div>
      <div className="dashboard-list">
        {query.data.items.map((dashboard) => (
          <GrafanaFrame key={dashboard.key} dashboard={dashboard} />
        ))}
      </div>
    </section>
  );
}
