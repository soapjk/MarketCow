import { useState, type FormEvent } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../lib/api";

type Provider = {
  provider: string; status: string; last_attempt_at: string; last_success_at?: string;
  last_error?: string; consecutive_failures: number; configured: boolean | null;
};
type ProviderPage = { items: Provider[]; page: { total: number } };
type HistoryJob = {
  job_id: string; status: string; progress_percent?: number; provider?: string;
  total_symbols?: number; rows_persisted?: number; updated_at: string;
};
type JobPage = { items: HistoryJob[]; page: { total: number } };

const defaultJob = {
  symbols: "AAPL,MSFT", provider: "yahoo", range: "1mo", interval: "1d",
  adjustment: "raw", allow_fallback: false, max_concurrency: 2, max_attempts: 3,
  retry_backoff_seconds: 0.5, canonical_wait_seconds: 5,
};

export function OperationsPage() {
  const client = useQueryClient();
  const [tab, setTab] = useState<"jobs" | "providers">("jobs");
  const [form, setForm] = useState(defaultJob);
  const providers = useQuery({
    queryKey: ["admin-providers"],
    queryFn: () => api.request<ProviderPage>("/v1/admin/providers?limit=100"),
    refetchInterval: 15_000,
  });
  const jobs = useQuery({
    queryKey: ["history-jobs"],
    queryFn: () => api.request<JobPage>("/v1/admin/history-jobs?limit=100"),
    refetchInterval: 2_000,
  });
  const createJob = useMutation({
    mutationFn: () => api.request("/v1/admin/history-jobs", {
      method: "POST",
      body: JSON.stringify({
        ...form,
        symbols: form.symbols.split(",").map((item) => item.trim().toUpperCase()).filter(Boolean),
        idempotency_key: `admin-${crypto.randomUUID()}`,
      }),
    }),
    onSuccess: () => client.invalidateQueries({ queryKey: ["history-jobs"] }),
  });
  const command = useMutation({
    mutationFn: ({ jobId, action }: { jobId: string; action: "cancel" | "retry-failed" }) =>
      api.request(`/v1/admin/history-jobs/${encodeURIComponent(jobId)}/${action}`, { method: "POST" }),
    onSuccess: () => client.invalidateQueries({ queryKey: ["history-jobs"] }),
  });
  function submit(event: FormEvent) {
    event.preventDefault();
    createJob.mutate();
  }
  function runCommand(jobId: string, action: "cancel" | "retry-failed") {
    const label = action === "cancel" ? "取消" : "重试失败项";
    if (window.confirm(`确认${label}任务 ${jobId.slice(0, 12)}？`)) command.mutate({ jobId, action });
  }
  return (
    <section>
      <div className="page-intro"><div><p className="eyebrow">OPERATIONS / AUDITED</p><h2>任务与服务</h2></div><div className="tab-switch"><button className={tab === "jobs" ? "active" : ""} onClick={() => setTab("jobs")}>历史任务</button><button className={tab === "providers" ? "active" : ""} onClick={() => setTab("providers")}>Provider</button></div></div>
      {tab === "jobs" ? (
        <div className="split-layout">
          <form className="data-card operation-form" onSubmit={submit}>
            <header><div><p className="eyebrow">NEW BATCH</p><h3>创建历史任务</h3></div></header>
            <label>标的（逗号分隔）<input value={form.symbols} onChange={(e) => setForm({ ...form, symbols: e.target.value })} required /></label>
            <div className="form-grid">
              <label>Provider<select value={form.provider} onChange={(e) => setForm({ ...form, provider: e.target.value })}><option>yahoo</option><option>tushare</option><option>hyperliquid</option></select></label>
              <label>范围<input value={form.range} onChange={(e) => setForm({ ...form, range: e.target.value })} /></label>
              <label>周期<input value={form.interval} onChange={(e) => setForm({ ...form, interval: e.target.value })} /></label>
              <label>复权<select value={form.adjustment} onChange={(e) => setForm({ ...form, adjustment: e.target.value })}><option>raw</option><option>adjusted</option></select></label>
            </div>
            <button className="primary-action" type="submit" disabled={createJob.isPending}>{createJob.isPending ? "提交中…" : "创建任务"}</button>
            {createJob.isError && <p className="inline-error">{createJob.error.message}</p>}
          </form>
          <article className="data-card">
            <header><div><p className="eyebrow">DURABLE JOBS</p><h3>任务列表</h3></div><span>{jobs.data?.page.total ?? 0} 项</span></header>
            {jobs.isError ? <p className="inline-error">{jobs.error.message}</p> : (
              <div className="job-list">{jobs.data?.items.map((job) => (
                <div className="job-row" key={job.job_id}>
                  <div><code>{job.job_id.slice(0, 12)}</code><span>{job.provider ?? "—"} · {job.total_symbols ?? 0} symbols · {job.rows_persisted ?? 0} rows</span></div>
                  <div className="job-progress"><span className={`status-pill status-${job.status}`}>{job.status}</span><progress max="100" value={job.progress_percent ?? 0} /></div>
                  <div className="row-actions">
                    {!["succeeded", "failed", "canceled", "partially_failed"].includes(job.status) && <button onClick={() => runCommand(job.job_id, "cancel")}>取消</button>}
                    {["failed", "partially_failed"].includes(job.status) && <button onClick={() => runCommand(job.job_id, "retry-failed")}>重试</button>}
                  </div>
                </div>
              ))}</div>
            )}
          </article>
        </div>
      ) : (
        <article className="data-card">
          <header><div><p className="eyebrow">SOURCE HEALTH</p><h3>Provider 状态</h3></div><span>{providers.data?.page.total ?? 0} 项</span></header>
          {providers.isError ? <p className="inline-error">{providers.error.message}</p> : <div className="table-wrap"><table><thead><tr><th>Provider</th><th>状态</th><th>最近成功</th><th>连续失败</th><th>最近错误</th></tr></thead><tbody>
            {providers.data?.items.map((item) => <tr key={item.provider}><td><strong>{item.provider}</strong></td><td><span className={`status-pill status-${item.status}`}>{item.status}</span></td><td>{item.last_success_at ? new Date(item.last_success_at).toLocaleString() : "—"}</td><td>{item.consecutive_failures}</td><td className="error-cell">{item.last_error || "—"}</td></tr>)}
          </tbody></table></div>}
        </article>
      )}
    </section>
  );
}
