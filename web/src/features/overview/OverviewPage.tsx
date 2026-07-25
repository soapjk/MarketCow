import { useQuery } from "@tanstack/react-query";
import { api } from "../../lib/api";

type Overview = {
  schema: string;
  generated_at: string;
  service: { status: string; version: string; profile: string };
  storage: { ready?: boolean; status?: string; components?: Record<string, unknown> };
  providers: { total: number; healthy: number; items: Record<string, unknown>[] };
  history_jobs: { items: { job_id: string; status: string; progress_percent?: number; updated_at: string }[] };
};

export function OverviewPage() {
  const query = useQuery({
    queryKey: ["admin-overview"],
    queryFn: () => api.request<Overview>("/v1/admin/overview"),
    refetchInterval: 10_000,
  });
  if (query.isPending) return <section className="page-state">正在汇总系统状态…</section>;
  if (query.isError) return <section className="page-state error-state"><strong>总览不可用</strong><span>{query.error.message}</span></section>;
  const data = query.data;
  const storageReady = data.storage.ready !== false;
  return (
    <section>
      <div className="page-intro">
        <div><p className="eyebrow">CONTROL PLANE / 10S</p><h2>运行总览</h2></div>
        <p>生成于 {new Date(data.generated_at).toLocaleString()}，页面仅展示控制面摘要。</p>
      </div>
      <div className="overview-grid">
        <article className="summary-card accent-card"><span>API</span><strong>{data.service.status}</strong><small>v{data.service.version} / {data.service.profile}</small></article>
        <article className="summary-card"><span>存储就绪</span><strong>{storageReady ? "READY" : "DEGRADED"}</strong><small>PostgreSQL + ClickHouse</small></article>
        <article className="summary-card"><span>Provider 健康</span><strong>{data.providers.healthy}/{data.providers.total}</strong><small>最近持久化状态</small></article>
        <article className="summary-card"><span>近期任务</span><strong>{data.history_jobs.items.length}</strong><small>最多显示 10 项</small></article>
      </div>
      <article className="data-card">
        <header><div><p className="eyebrow">RECENT WORK</p><h3>历史任务</h3></div><a href="#/operations">打开任务中心 →</a></header>
        {data.history_jobs.items.length ? (
          <div className="table-wrap"><table><thead><tr><th>任务</th><th>状态</th><th>进度</th><th>更新时间</th></tr></thead>
            <tbody>{data.history_jobs.items.map((job) => <tr key={job.job_id}><td><code>{job.job_id.slice(0, 12)}</code></td><td><span className={`status-pill status-${job.status}`}>{job.status}</span></td><td>{job.progress_percent ?? 0}%</td><td>{new Date(job.updated_at).toLocaleString()}</td></tr>)}</tbody>
          </table></div>
        ) : <p className="empty-copy">当前没有历史任务。</p>}
      </article>
    </section>
  );
}
