import { useQuery } from "@tanstack/react-query";
import { api } from "../../lib/api";

type AuditPage = {
  durable: boolean;
  items: {
    audit_id: string; occurred_at: string; actor: string; action: string;
    target: string; outcome: string; detail: string;
  }[];
};

export function SettingsPage() {
  const audit = useQuery({
    queryKey: ["admin-audit"],
    queryFn: () => api.request<AuditPage>("/v1/admin/audit?limit=100"),
    refetchInterval: 10_000,
  });
  return (
    <section>
      <div className="page-intro"><div><p className="eyebrow">ADMINISTRATION / TRACE</p><h2>管理设置与审计</h2></div><p>看板注册由本地环境配置管理；此页只展示经过脱敏的操作证据。</p></div>
      <article className="data-card">
        <header><div><p className="eyebrow">APPEND ONLY</p><h3>操作审计</h3></div><span>{audit.data?.durable ? "PostgreSQL durable" : "Memory fallback"}</span></header>
        {audit.isPending ? <p className="empty-copy">正在读取审计记录…</p>
          : audit.isError ? <p className="inline-error">{audit.error.message}</p>
          : audit.data.items.length ? <div className="table-wrap"><table><thead><tr><th>时间</th><th>操作者</th><th>动作</th><th>目标</th><th>结果</th><th>说明</th></tr></thead><tbody>
            {audit.data.items.map((item) => <tr key={item.audit_id}><td>{new Date(item.occurred_at).toLocaleString()}</td><td>{item.actor}</td><td><code>{item.action}</code></td><td><code>{item.target}</code></td><td><span className={`status-pill status-${item.outcome}`}>{item.outcome}</span></td><td>{item.detail || "—"}</td></tr>)}
          </tbody></table></div> : <p className="empty-copy">暂无管理操作记录。</p>}
      </article>
    </section>
  );
}
