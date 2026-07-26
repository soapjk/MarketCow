import { useState, type ChangeEvent } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../lib/api";
import { useIdentity } from "../auth/authContext";

type UploadReceipt = {
  upload_id: string;
  filename: string;
  byte_size: number;
  sha256: string;
  columns: string[];
  delimiter: string;
};

type DryRunReport = {
  status: string;
  rows_total?: number;
  rows_valid?: number;
  rows_invalid?: number;
  duplicate_rows?: number;
  unordered_rows?: number;
  first_bar_at?: string;
  last_bar_at?: string;
  errors?: unknown[];
};

type SemanticInference = {
  schema: string;
  sample: {
    rows_seen: number;
    timestamps_sampled: number;
    timestamp_parse_failures: number;
  };
  timezone: {
    value: string | null;
    confidence: "high" | "medium" | "low" | "none";
    score: number;
    evidence: string[];
    alternatives: { value: string; score: number }[];
  };
  adjustment: {
    value: "raw" | "qfq" | "hfq" | null;
    confidence: "high" | "medium" | "low" | "none";
    score: number;
    evidence: string[];
  };
};

type CsvJob = {
  job_id: string;
  status: string;
  progress_percent?: number;
  rows_total?: number;
  rows_read?: number;
  rows_written?: number;
  updated_at: string;
  quality_report_json?: { status?: string };
  error_code?: string;
  error_message?: string;
};

type JobPage = { count: number; items: CsvJob[] };

const micOptions = [
  ["XNAS", "NASDAQ"],
  ["XNYS", "New York Stock Exchange"],
  ["ARCX", "NYSE Arca"],
  ["XHKG", "Hong Kong Exchanges"],
  ["XSHG", "Shanghai Stock Exchange"],
  ["XSHE", "Shenzhen Stock Exchange"],
  ["XBSE", "Beijing Stock Exchange"],
] as const;

const defaultColumns = {
  timestamp: "timestamp",
  open: "open",
  high: "high",
  low: "low",
  close: "close",
  volume: "volume",
};
type ColumnMapping = typeof defaultColumns;

const columnAliases: Record<keyof ColumnMapping, string[]> = {
  timestamp: ["timestamp", "datetime", "date_time", "date", "time", "bar_at"],
  open: ["open", "open_price", "price_open"],
  high: ["high", "high_price", "price_high"],
  low: ["low", "low_price", "price_low"],
  close: ["close", "close_price", "price_close"],
  volume: ["volume", "vol", "trade_volume"],
};

function normalizedColumn(value: string) {
  return value.trim().toLowerCase().replace(/[^a-z0-9]+/g, "_");
}

function detectColumnMapping(columns: string[], fallback: ColumnMapping) {
  return Object.fromEntries(
    (Object.keys(columnAliases) as (keyof ColumnMapping)[]).map((field) => {
      const match = columns.find((column) => (
        columnAliases[field].includes(normalizedColumn(column))
      ));
      return [field, match ?? fallback[field]];
    }),
  ) as ColumnMapping;
}

function message(error: unknown) {
  return error instanceof Error ? error.message : String(error);
}

function formatBytes(value: number) {
  if (value < 1024) return `${value} B`;
  if (value < 1024 ** 2) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / 1024 ** 2).toFixed(1)} MB`;
}

export function CsvImportsPage() {
  const identity = useIdentity();
  const canOperate = identity?.role === "operator" || identity?.role === "admin";
  const queryClient = useQueryClient();
  const [file, setFile] = useState<File | null>(null);
  const [upload, setUpload] = useState<UploadReceipt | null>(null);
  const [dryRun, setDryRun] = useState<DryRunReport | null>(null);
  const [source, setSource] = useState("purchased_vendor");
  const [externalSymbol, setExternalSymbol] = useState("AAPL.US");
  const [symbol, setSymbol] = useState("AAPL");
  const [mic, setMic] = useState("XNAS");
  const [interval, setInterval] = useState("1m");
  const [adjustment, setAdjustment] = useState("raw");
  const [timezone, setTimezone] = useState("America/New_York");
  const [columnMapping, setColumnMapping] = useState<ColumnMapping>(defaultColumns);
  const [delimiter, setDelimiter] = useState(",");
  const [validatedDeclaration, setValidatedDeclaration] = useState("");
  const [inference, setInference] = useState<SemanticInference | null>(null);
  const [inferenceKey, setInferenceKey] = useState("");
  const [semanticsConfirmed, setSemanticsConfirmed] = useState(false);
  const [error, setError] = useState("");

  const jobs = useQuery({
    queryKey: ["csv-import-jobs"],
    queryFn: () => api.request<JobPage>("/v1/admin/csv-imports?limit=50"),
    refetchInterval: 2000,
  });

  const buildDeclaration = (
    columns: ColumnMapping = columnMapping,
    selectedDelimiter: string = delimiter,
    inferredTimezone: string = timezone,
    inferredAdjustment: string = adjustment,
  ) => ({
    source,
    interval,
    adjustment: inferredAdjustment,
    profile: {
      name: source,
      version: "1",
      columns,
      timezone_name: inferredTimezone,
      timestamp_format: "iso8601",
      encoding: "utf-8-sig",
      delimiter: selectedDelimiter,
      fixed_external_symbol: externalSymbol,
    },
    instruments: {
      namespace: `provider:${source}`,
      symbols: { [externalSymbol]: `${symbol.trim().toUpperCase()}.${mic}` },
    },
    created_by: identity?.actor ?? "admin-ui",
    source_proof: "operator-uploaded",
  });

  const ensureUpload = async () => {
    if (upload) {
      return { receipt: upload, columns: columnMapping, delimiter };
    }
    if (!file) throw new Error("请先选择 CSV 文件");
    const receipt = await api.request<UploadReceipt>(
      `/v1/admin/csv-imports/upload?filename=${encodeURIComponent(file.name)}`,
      {
        method: "POST",
        body: file,
        headers: { "Content-Type": "application/octet-stream" },
      },
    );
    const detectedColumns = detectColumnMapping(receipt.columns, columnMapping);
    setColumnMapping(detectedColumns);
    setDelimiter(receipt.delimiter);
    setUpload(receipt);
    return {
      receipt,
      columns: detectedColumns,
      delimiter: receipt.delimiter,
    };
  };

  const preflight = useMutation({
    mutationFn: async () => {
      setError("");
      const uploaded = await ensureUpload();
      const canonicalInstrumentId = `${symbol.trim().toUpperCase()}.${mic}`;
      const semanticKey = JSON.stringify({
        upload_id: uploaded.receipt.upload_id,
        instrument_id: canonicalInstrumentId,
        columns: uploaded.columns,
        delimiter: uploaded.delimiter,
      });
      let selectedTimezone = timezone;
      let selectedAdjustment = adjustment;
      if (!inference || inferenceKey !== semanticKey) {
        const inferred = await api.request<SemanticInference>(
          "/v1/admin/csv-imports/infer", {
            method: "POST",
            body: JSON.stringify({
              upload_id: uploaded.receipt.upload_id,
              instrument_id: canonicalInstrumentId,
              columns: uploaded.columns,
              timestamp_format: "iso8601",
              encoding: "utf-8-sig",
              delimiter: uploaded.delimiter,
            }),
          },
        );
        setInference(inferred);
        setInferenceKey(semanticKey);
        setSemanticsConfirmed(false);
        if (inferred.timezone.value) {
          selectedTimezone = inferred.timezone.value;
          setTimezone(inferred.timezone.value);
        }
        if (inferred.adjustment.value) {
          selectedAdjustment = inferred.adjustment.value;
          setAdjustment(inferred.adjustment.value);
        }
      }
      const selectedDeclaration = buildDeclaration(
        uploaded.columns, uploaded.delimiter,
        selectedTimezone, selectedAdjustment,
      );
      const report = await api.request<DryRunReport>(
        "/v1/admin/csv-imports/dry-run", {
        method: "POST",
        body: JSON.stringify({
          upload_id: uploaded.receipt.upload_id,
          declaration: selectedDeclaration,
          max_error_samples: 100,
        }),
      });
      return {
        report,
        declaration: JSON.stringify(selectedDeclaration),
      };
    },
    onSuccess: (result) => {
      setDryRun(result.report);
      setValidatedDeclaration(result.declaration);
    },
    onError: (value) => setError(message(value)),
  });

  const createImport = useMutation({
    mutationFn: async () => {
      if (
        !upload
        || dryRun?.status !== "valid"
        || validatedDeclaration !== JSON.stringify(buildDeclaration())
        || (
          inference
          && (
            ["low", "none"].includes(inference.timezone.confidence)
            || ["low", "none"].includes(inference.adjustment.confidence)
          )
          && !semanticsConfirmed
        )
      ) {
        throw new Error("必须先通过预检");
      }
      return api.request<{ created: boolean; job: CsvJob }>(
        "/v1/admin/csv-imports",
        {
          method: "POST",
          body: JSON.stringify({
            upload_id: upload.upload_id,
            declaration: buildDeclaration(),
            idempotency_key: `csv-upload-${upload.sha256}`,
            chunk_rows: 100000,
            max_attempts: 3,
          }),
        },
      );
    },
    onSuccess: async () => {
      setFile(null);
      setUpload(null);
      setDryRun(null);
      setValidatedDeclaration("");
      setInference(null);
      setInferenceKey("");
      setSemanticsConfirmed(false);
      await queryClient.invalidateQueries({ queryKey: ["csv-import-jobs"] });
    },
    onError: (value) => setError(message(value)),
  });

  const jobAction = useMutation({
    mutationFn: ({ job, action }: { job: CsvJob; action: "cancel" | "retry" }) => {
      const body = action === "retry"
        ? JSON.stringify({ idempotency_key: `csv-retry-${job.job_id}-${Date.now()}` })
        : undefined;
      return api.request(
        `/v1/admin/csv-imports/${encodeURIComponent(job.job_id)}/${action}`,
        { method: "POST", body },
      );
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["csv-import-jobs"] }),
    onError: (value) => setError(message(value)),
  });

  const chooseFile = (event: ChangeEvent<HTMLInputElement>) => {
    setFile(event.target.files?.[0] ?? null);
    setUpload(null);
    setDryRun(null);
    setValidatedDeclaration("");
    setInference(null);
    setInferenceKey("");
    setSemanticsConfirmed(false);
    setColumnMapping(defaultColumns);
    setDelimiter(",");
    setError("");
  };

  const canonicalId = `${symbol.trim().toUpperCase()}.${mic}`;
  const validForm = Boolean(
    file && source.trim() && externalSymbol.trim() && symbol.trim() && mic,
  );
  const preflightMatches = dryRun?.status === "valid"
    && validatedDeclaration === JSON.stringify(buildDeclaration());
  const requiresSemanticConfirmation = Boolean(
    inference && (
      ["low", "none"].includes(inference.timezone.confidence)
      || ["low", "none"].includes(inference.adjustment.confidence)
    ),
  );

  return (
    <>
      <section className="page-intro">
        <div>
          <p className="eyebrow">INGEST / CSV</p>
          <h2>上传历史 K 线</h2>
        </div>
        <p>
          文件先进入本地受控暂存区，通过预检后才会创建可恢复的分片导入任务。
          MarketCow 内部标识始终使用 SYMBOL.MIC。
        </p>
      </section>

      <div className="csv-import-layout">
        <section className="data-card csv-upload-form">
          <header>
            <div><p className="eyebrow">01 / FILE</p><h3>文件与数据身份</h3></div>
            <span>CSV ONLY</span>
          </header>

          <label className={`file-drop ${file ? "has-file" : ""}`}>
            <input type="file" accept=".csv,text/csv" onChange={chooseFile} />
            <strong>{file ? file.name : "选择 CSV 文件"}</strong>
            <span>{file ? formatBytes(file.size) : "点击浏览本机文件；上传过程不会占用整文件内存"}</span>
          </label>

          <div className="form-grid csv-column-grid">
            <label>数据供应商
              <input value={source} onChange={(event) => setSource(event.target.value)} />
            </label>
            <label>CSV 中的标的代码
              <input
                aria-label="CSV 中的标的代码"
                value={externalSymbol}
                onChange={(event) => setExternalSymbol(event.target.value)}
              />
            </label>
            <label>标准 Symbol
              <input
                aria-label="标准 Symbol"
                value={symbol}
                onChange={(event) => setSymbol(event.target.value)}
              />
            </label>
            <label>MIC
              <select aria-label="MIC" value={mic} onChange={(event) => setMic(event.target.value)}>
                {micOptions.map(([value, label]) => (
                  <option key={value} value={value}>{value} — {label}</option>
                ))}
              </select>
            </label>
          </div>

          <div className="identity-preview">
            <span>统一 Instrument ID</span>
            <strong>{canonicalId}</strong>
            <small>{externalSymbol} → {canonicalId}</small>
          </div>

          <header className="subsection-header">
            <div><p className="eyebrow">02 / SCHEMA</p><h3>CSV 格式</h3></div>
          </header>
          <div className="form-grid">
            <label>周期
              <select value={interval} onChange={(event) => setInterval(event.target.value)}>
                {["1m", "5m", "15m", "30m", "1h", "1d"].map((value) => (
                  <option key={value}>{value}</option>
                ))}
              </select>
            </label>
            <label>复权状态
              <select value={adjustment} onChange={(event) => setAdjustment(event.target.value)}>
                <option value="raw">raw — 未复权</option>
                <option value="qfq">qfq — 前复权</option>
                <option value="hfq">hfq — 后复权</option>
              </select>
            </label>
            <label>CSV 时区
              <input value={timezone} onChange={(event) => setTimezone(event.target.value)} />
            </label>
            {(Object.keys(columnMapping) as (keyof ColumnMapping)[]).map((field) => (
              <label key={field}>{field === "timestamp" ? "时间列" : `${field} 列`}
                {upload?.columns.length ? (
                  <select
                    aria-label={`${field} 列`}
                    value={columnMapping[field]}
                    onChange={(event) => setColumnMapping({
                      ...columnMapping, [field]: event.target.value,
                    })}
                  >
                    {upload.columns.map((column) => (
                      <option key={column} value={column}>{column}</option>
                    ))}
                  </select>
                ) : (
                  <input
                    aria-label={`${field} 列`}
                    value={columnMapping[field]}
                    onChange={(event) => setColumnMapping({
                      ...columnMapping, [field]: event.target.value,
                    })}
                  />
                )}
              </label>
            ))}
          </div>
          <p className="field-hint">
            上传后会自动识别常见列名和大小写；如果供应商命名特殊，可在这里调整
            时间、OHLC 和成交量映射。当前文件按单一标的导入，因此标的代码不必
            作为 CSV 列存在。
          </p>

          {inference ? (
            <section className="semantic-inference" aria-label="自动语义分析">
              <header>
                <div>
                  <p className="eyebrow">AUTO INFERENCE</p>
                  <h3>自动语义分析</h3>
                </div>
                <span>基于 {inference.sample.timestamps_sampled.toLocaleString()} 个时间点</span>
              </header>
              <div className="inference-grid">
                <article>
                  <div>
                    <span>源时区建议</span>
                    <strong>{inference.timezone.value ?? "无法判断"}</strong>
                    <i className={`confidence confidence-${inference.timezone.confidence}`}>
                      {inference.timezone.confidence} · 匹配分 {(inference.timezone.score * 100).toFixed(0)}
                    </i>
                  </div>
                  <ul>{inference.timezone.evidence.map((item) => <li key={item}>{item}</li>)}</ul>
                </article>
                <article>
                  <div>
                    <span>复权状态建议</span>
                    <strong>{inference.adjustment.value ?? "无法判断"}</strong>
                    <i className={`confidence confidence-${inference.adjustment.confidence}`}>
                      {inference.adjustment.confidence} · 证据分 {(inference.adjustment.score * 100).toFixed(0)}
                    </i>
                  </div>
                  <ul>{inference.adjustment.evidence.map((item) => <li key={item}>{item}</li>)}</ul>
                </article>
              </div>
              <p>
                建议值已预填。高置信度仍是推断；低置信度项目请结合供应商说明人工确认。
                修改设置后必须重新预检。
              </p>
              {requiresSemanticConfirmation ? (
                <label className="semantic-confirmation">
                  <input
                    type="checkbox"
                    checked={semanticsConfirmed}
                    onChange={(event) => setSemanticsConfirmed(event.target.checked)}
                  />
                  我已核对供应商说明，并确认采用当前低置信度设置
                </label>
              ) : null}
            </section>
          ) : null}

          {error ? <p className="inline-error" role="alert">{error}</p> : null}
          <div className="csv-actions">
            <button
              type="button"
              className="secondary-action"
              disabled={!canOperate || !validForm || preflight.isPending}
              onClick={() => preflight.mutate()}
            >
              {preflight.isPending ? "上传并检查中…" : "上传并预检"}
            </button>
            <button
              type="button"
              className="primary-action"
              disabled={
                !canOperate || !preflightMatches || createImport.isPending
                || (requiresSemanticConfirmation && !semanticsConfirmed)
              }
              onClick={() => createImport.mutate()}
            >
              {createImport.isPending ? "正在创建…" : "开始正式导入"}
            </button>
          </div>
        </section>

        <aside className="data-card csv-preflight">
          <header>
            <div><p className="eyebrow">PREFLIGHT</p><h3>预检结果</h3></div>
            <span className={`status-pill status-${dryRun?.status ?? "pending"}`}>
              {dryRun?.status ?? "等待"}
            </span>
          </header>
          {upload ? (
            <dl className="receipt-grid">
              <div><dt>文件</dt><dd>{upload.filename}</dd></div>
              <div><dt>大小</dt><dd>{formatBytes(upload.byte_size)}</dd></div>
              <div><dt>SHA-256</dt><dd title={upload.sha256}>{upload.sha256.slice(0, 16)}…</dd></div>
              <div><dt>有效行</dt><dd>{dryRun?.rows_valid ?? "—"}</dd></div>
              <div><dt>无效行</dt><dd>{dryRun?.rows_invalid ?? "—"}</dd></div>
              <div><dt>重复行</dt><dd>{dryRun?.duplicate_rows ?? "—"}</dd></div>
              <div><dt>首条时间</dt><dd>{dryRun?.first_bar_at ?? "—"}</dd></div>
              <div><dt>末条时间</dt><dd>{dryRun?.last_bar_at ?? "—"}</dd></div>
            </dl>
          ) : <p className="empty-copy">选择文件并执行预检后，这里会显示质量摘要。</p>}
        </aside>
      </div>

      <section className="data-card csv-job-card">
        <header>
          <div><p className="eyebrow">RECENT JOBS</p><h3>CSV 导入任务</h3></div>
          <span>每 2 秒刷新</span>
        </header>
        <div className="table-wrap">
          <table>
            <thead><tr>
              <th>任务</th><th>状态</th><th>进度</th><th>写入行</th>
              <th>质量门禁</th><th>更新时间</th><th>操作</th>
            </tr></thead>
            <tbody>
              {(jobs.data?.items ?? []).map((job) => (
                <tr key={job.job_id}>
                  <td><code title={job.job_id}>{job.job_id.slice(0, 12)}</code></td>
                  <td><span className={`status-pill status-${job.status}`}>{job.status}</span></td>
                  <td className="csv-progress">
                    <progress max="100" value={job.progress_percent ?? 0} />
                    <span>{job.progress_percent ?? 0}%</span>
                  </td>
                  <td>{job.rows_written ?? 0} / {job.rows_total ?? 0}</td>
                  <td>{job.quality_report_json?.status ?? "pending"}</td>
                  <td>{job.updated_at}</td>
                  <td className="row-actions">
                    {["queued", "running", "cancel_requested"].includes(job.status) ? (
                      <button
                        type="button"
                        disabled={!canOperate}
                        onClick={() => jobAction.mutate({ job, action: "cancel" })}
                      >取消</button>
                    ) : ["failed", "canceled"].includes(job.status) ? (
                      <button
                        type="button"
                        disabled={!canOperate}
                        onClick={() => jobAction.mutate({ job, action: "retry" })}
                      >重试</button>
                    ) : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {!jobs.isLoading && !jobs.data?.items.length
            ? <p className="empty-copy">暂无 CSV 导入任务。</p> : null}
        </div>
      </section>
    </>
  );
}
