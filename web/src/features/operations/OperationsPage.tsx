import { useState, type FormEvent } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../lib/api";
import { useIdentity } from "../auth/authContext";

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

type JobProvider = "yahoo" | "tushare" | "hyperliquid";
type JobForm = {
  symbols: string; provider: JobProvider; startDate: string; endDate: string;
  interval: string; adjustment: string; allow_fallback: boolean;
  max_concurrency: number; max_attempts: number; retry_backoff_seconds: number;
  retry_max_backoff_seconds: number; retry_jitter_seconds: number;
  retry_budget_seconds: number; canonical_wait_seconds: number;
};

const providerIntervals: Record<JobProvider, { value: string; label: string }[]> = {
  yahoo: [
    ["1m", "1 分钟"], ["2m", "2 分钟"], ["5m", "5 分钟"],
    ["15m", "15 分钟"], ["30m", "30 分钟"], ["60m", "60 分钟"],
    ["90m", "90 分钟"], ["1h", "1 小时"], ["1d", "日线"],
    ["5d", "5 日"], ["1wk", "周线"], ["1mo", "月线"], ["3mo", "季线"],
  ].map(([value, label]) => ({ value, label })),
  tushare: [
    ["1m", "1 分钟"], ["5m", "5 分钟"], ["15m", "15 分钟"],
    ["30m", "30 分钟"], ["60m", "60 分钟"], ["1h", "1 小时"],
  ].map(([value, label]) => ({ value, label })),
  hyperliquid: [
    ["1m", "1 分钟"], ["5m", "5 分钟"], ["15m", "15 分钟"],
    ["30m", "30 分钟"], ["1h", "1 小时"], ["1d", "日线"],
  ].map(([value, label]) => ({ value, label })),
};

function inputDate(value: Date) {
  const year = value.getFullYear();
  const month = String(value.getMonth() + 1).padStart(2, "0");
  const day = String(value.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function createDefaultJob(): JobForm {
  const end = new Date();
  const start = new Date(end);
  start.setDate(start.getDate() - 30);
  return {
  symbols: "AAPL,MSFT", provider: "yahoo",
  startDate: inputDate(start), endDate: inputDate(end), interval: "1d",
  adjustment: "raw", allow_fallback: false, max_concurrency: 2, max_attempts: 3,
  retry_backoff_seconds: 0.5, retry_max_backoff_seconds: 30,
  retry_jitter_seconds: 0.5, retry_budget_seconds: 120,
  canonical_wait_seconds: 5,
  };
}

export function OperationsPage({ initialTab = "jobs" }: { initialTab?: "jobs" | "providers" }) {
  const identity = useIdentity();
  const canOperate = identity?.role === "operator" || identity?.role === "admin";
  const client = useQueryClient();
  const tab = initialTab;
  const [form, setForm] = useState<JobForm>(createDefaultJob);
  const dateError = !form.startDate || !form.endDate || form.startDate > form.endDate;
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
      body: JSON.stringify((() => {
        const { startDate, endDate, ...request } = form;
        return {
        ...request,
        range: "custom",
        range_start: `${startDate}T00:00:00.000Z`,
        range_end: `${endDate}T23:59:59.999Z`,
        symbols: form.symbols.split(",").map((item) => item.trim().toUpperCase()).filter(Boolean),
        idempotency_key: `admin-${crypto.randomUUID()}`,
        };
      })()),
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
    if (dateError) return;
    createJob.mutate();
  }
  function selectProvider(provider: JobProvider) {
    const intervals = providerIntervals[provider];
    const interval = intervals.some((item) => item.value === form.interval)
      ? form.interval : intervals[0].value;
    setForm({
      ...form,
      provider,
      interval,
      adjustment: provider === "yahoo" ? form.adjustment : "raw",
    });
  }
  function runCommand(jobId: string, action: "cancel" | "retry-failed") {
    const label = action === "cancel" ? "取消" : "重试失败项";
    if (window.confirm(`确认${label}任务 ${jobId.slice(0, 12)}？`)) command.mutate({ jobId, action });
  }
  function selectTab(next: "jobs" | "providers") {
    window.location.hash = next === "providers" ? "#/operations/providers" : "#/operations";
  }
  return (
    <section>
      <div className="page-intro"><div><p className="eyebrow">OPERATIONS / AUDITED</p><h2>任务与服务</h2></div><div className="tab-switch"><button className={tab === "jobs" ? "active" : ""} onClick={() => selectTab("jobs")}>历史任务</button><button className={tab === "providers" ? "active" : ""} onClick={() => selectTab("providers")}>Provider</button></div></div>
      {tab === "jobs" ? (
        <div className="split-layout">
          <form className="data-card operation-form" onSubmit={submit} aria-disabled={!canOperate}>
            <header><div><p className="eyebrow">NEW BATCH</p><h3>创建历史任务</h3></div></header>
            <label>标的（逗号分隔）<input value={form.symbols} onChange={(e) => setForm({ ...form, symbols: e.target.value })} required /></label>
            <div className="form-grid">
              <label>Provider<select value={form.provider} onChange={(e) => selectProvider(e.target.value as JobProvider)}><option>yahoo</option><option>tushare</option><option>hyperliquid</option></select></label>
              <label>周期<select value={form.interval} onChange={(e) => setForm({ ...form, interval: e.target.value })}>{providerIntervals[form.provider].map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}</select></label>
              <label>开始日期<input type="date" value={form.startDate} max={form.endDate} onChange={(e) => setForm({ ...form, startDate: e.target.value })} required /></label>
              <label>结束日期<input type="date" value={form.endDate} min={form.startDate} max={inputDate(new Date())} onChange={(e) => setForm({ ...form, endDate: e.target.value })} required /></label>
              <label>复权<select value={form.adjustment} disabled={form.provider !== "yahoo"} onChange={(e) => setForm({ ...form, adjustment: e.target.value })}><option>raw</option><option>adjusted</option></select></label>
            </div>
            <p className="field-hint">日期按 UTC 自然日提交；结束日期包含当天。周期选项会根据 Provider 自动收窄。</p>
            {dateError && <p className="inline-error">结束日期不能早于开始日期。</p>}
            <button className="primary-action" type="submit" disabled={!canOperate || createJob.isPending || dateError}>{createJob.isPending ? "提交中…" : canOperate ? "创建任务" : "Viewer 无操作权限"}</button>
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
                    {canOperate && !["succeeded", "failed", "canceled", "partially_failed"].includes(job.status) && <button onClick={() => runCommand(job.job_id, "cancel")}>取消</button>}
                    {canOperate && ["failed", "partially_failed"].includes(job.status) && <button onClick={() => runCommand(job.job_id, "retry-failed")}>重试</button>}
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
