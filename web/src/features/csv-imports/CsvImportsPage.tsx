import { useState, type ChangeEvent } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, ApiError } from "../../lib/api";
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
  phase?: string;
  heartbeat_at?: string | null;
  updated_at: string;
  quality_report_json?: { status?: string };
  error_code?: string;
  error_message?: string;
};

type JobPage = { count: number; items: CsvJob[] };

type Declaration = ReturnType<typeof declarationShape>;
type BatchStage =
  | "selected"
  | "uploading"
  | "preflighting"
  | "valid"
  | "invalid"
  | "creating"
  | "created"
  | "failed";

type BatchItem = {
  key: string;
  path: string;
  file: File;
  externalSymbol: string;
  symbol: string;
  stage: BatchStage;
  receipt?: UploadReceipt;
  report?: DryRunReport;
  declaration?: Declaration;
  jobId?: string;
  error?: string;
};

type PreflightMutationResult =
  | { mode: "single"; report: DryRunReport; declaration: string }
  | { mode: "batch"; items: BatchItem[] };

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

function authenticationFailed(error: unknown) {
  return error instanceof ApiError && error.status === 401;
}

function formatBytes(value: number) {
  if (value < 1024) return `${value} B`;
  if (value < 1024 ** 2) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / 1024 ** 2).toFixed(1)} MB`;
}

function formatRows(value: number | undefined) {
  return new Intl.NumberFormat("zh-CN").format(value ?? 0);
}

const phaseLabels: Record<string, string> = {
  queued: "排队中",
  importing: "读取并写入",
  verifying: "质量检查中",
  canceling: "正在取消",
  completed: "已完成",
  failed: "失败",
  canceled: "已取消",
};

function phaseLabel(job: CsvJob) {
  return phaseLabels[job.phase ?? ""] ?? job.status;
}

function isStalled(job: CsvJob) {
  if (job.phase !== "importing") return false;
  const updated = Date.parse(job.heartbeat_at ?? job.updated_at);
  return Number.isFinite(updated) && Date.now() - updated > 60_000;
}

function declarationShape(
  source: string,
  interval: string,
  adjustment: string,
  columns: ColumnMapping,
  timezone: string,
  delimiter: string,
  externalSymbol: string,
  canonicalInstrumentId: string,
  actor: string,
) {
  return {
    source,
    interval,
    adjustment,
    profile: {
      name: source,
      version: "1",
      columns,
      timezone_name: timezone,
      timestamp_format: "iso8601",
      encoding: "utf-8-sig",
      delimiter,
      fixed_external_symbol: externalSymbol,
    },
    instruments: {
      namespace: `provider:${source}`,
      symbols: { [externalSymbol]: canonicalInstrumentId },
    },
    created_by: actor,
    source_proof: "operator-uploaded",
  };
}

function filePath(file: File) {
  return file.webkitRelativePath || file.name;
}

function fileIdentity(file: File) {
  const externalSymbol = file.name.replace(/\.csv$/i, "").trim();
  const symbol = externalSymbol
    .replace(/\.(US|HK|SH|SZ|BJ)$/i, "")
    .trim()
    .toUpperCase();
  return { externalSymbol, symbol };
}

function selectedCsvFiles(files: FileList | null): BatchItem[] {
  return Array.from(files ?? [])
    .filter((candidate) => candidate.name.toLowerCase().endsWith(".csv"))
    .sort((left, right) => filePath(left).localeCompare(filePath(right)))
    .map((candidate) => ({
      key: filePath(candidate),
      path: filePath(candidate),
      file: candidate,
      ...fileIdentity(candidate),
      stage: "selected" as const,
    }));
}

const batchStageLabels: Record<BatchStage, string> = {
  selected: "等待",
  uploading: "上传中",
  preflighting: "预检中",
  valid: "预检通过",
  invalid: "预检失败",
  creating: "创建任务中",
  created: "任务已创建",
  failed: "失败",
};

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
  const [batchItems, setBatchItems] = useState<BatchItem[]>([]);

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
    selectedExternalSymbol: string = externalSymbol,
    selectedSymbol: string = symbol,
  ) => declarationShape(
    source,
    interval,
    inferredAdjustment,
    columns,
    inferredTimezone,
    selectedDelimiter,
    selectedExternalSymbol,
    `${selectedSymbol.trim().toUpperCase()}.${mic}`,
    identity?.actor ?? "admin-ui",
  );

  const updateBatchItem = (
    key: string,
    patch: Partial<BatchItem>,
  ) => {
    setBatchItems((current) => current.map((item) => (
      item.key === key ? { ...item, ...patch } : item
    )));
  };

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

  const inferSemantics = async (
    receipt: UploadReceipt,
    columns: ColumnMapping,
    selectedDelimiter: string,
    canonicalInstrumentId: string,
  ) => {
    const semanticKey = JSON.stringify({
      upload_id: receipt.upload_id,
      instrument_id: canonicalInstrumentId,
      columns,
      delimiter: selectedDelimiter,
    });
    let selectedTimezone = timezone;
    let selectedAdjustment = adjustment;
    if (!inference || inferenceKey !== semanticKey) {
      const inferred = await api.request<SemanticInference>(
        "/v1/admin/csv-imports/infer", {
          method: "POST",
          body: JSON.stringify({
            upload_id: receipt.upload_id,
            instrument_id: canonicalInstrumentId,
            columns,
            timestamp_format: "iso8601",
            encoding: "utf-8-sig",
            delimiter: selectedDelimiter,
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
    return { selectedTimezone, selectedAdjustment };
  };

  const preflight = useMutation({
    mutationFn: async (): Promise<PreflightMutationResult> => {
      setError("");
      if (batchItems.length) {
        const completed: BatchItem[] = [];
        let sharedColumns: ColumnMapping | null = null;
        let sharedSourceColumns: string[] | null = null;
        let sharedDelimiter = delimiter;
        let selectedTimezone = timezone;
        let selectedAdjustment = adjustment;

        for (const original of batchItems) {
          let current: BatchItem = {
            ...original,
            stage: "uploading",
            receipt: undefined,
            report: undefined,
            declaration: undefined,
            jobId: undefined,
            error: undefined,
          };
          updateBatchItem(current.key, current);
          try {
            const receipt = await api.request<UploadReceipt>(
              `/v1/admin/csv-imports/upload?filename=${
                encodeURIComponent(current.file.name)
              }`,
              {
                method: "POST",
                body: current.file,
                headers: { "Content-Type": "application/octet-stream" },
              },
            );
            if (sharedColumns === null) {
              sharedSourceColumns = receipt.columns;
              sharedColumns = detectColumnMapping(
                receipt.columns, columnMapping
              );
              sharedDelimiter = receipt.delimiter;
              setColumnMapping(sharedColumns);
              setDelimiter(sharedDelimiter);
              const inferred = await inferSemantics(
                receipt,
                sharedColumns,
                sharedDelimiter,
                `${current.symbol}.${mic}`,
              );
              selectedTimezone = inferred.selectedTimezone;
              selectedAdjustment = inferred.selectedAdjustment;
            } else if (
              receipt.delimiter !== sharedDelimiter
              || JSON.stringify(receipt.columns)
                !== JSON.stringify(sharedSourceColumns)
            ) {
              throw new Error("文件表头或分隔符与文件夹中的第一个 CSV 不一致");
            }
            const declaration = buildDeclaration(
              sharedColumns,
              sharedDelimiter,
              selectedTimezone,
              selectedAdjustment,
              current.externalSymbol,
              current.symbol,
            );
            current = {
              ...current,
              stage: "preflighting",
              receipt,
              declaration,
            };
            updateBatchItem(current.key, current);
            const report = await api.request<DryRunReport>(
              "/v1/admin/csv-imports/dry-run", {
                method: "POST",
                body: JSON.stringify({
                  upload_id: receipt.upload_id,
                  declaration,
                  max_error_samples: 100,
                }),
              },
            );
            current = {
              ...current,
              report,
              stage: report.status === "valid" ? "valid" : "invalid",
              error: report.status === "valid"
                ? undefined
                : "CSV 数据未通过预检",
            };
          } catch (value) {
            current = {
              ...current,
              stage: "failed",
              error: message(value),
            };
            updateBatchItem(current.key, current);
            if (authenticationFailed(value)) {
              throw value;
            }
          }
          completed.push(current);
          updateBatchItem(current.key, current);
        }
        return { mode: "batch", items: completed };
      }

      const uploaded = await ensureUpload();
      const canonicalInstrumentId = `${symbol.trim().toUpperCase()}.${mic}`;
      const { selectedTimezone, selectedAdjustment } = await inferSemantics(
        uploaded.receipt,
        uploaded.columns,
        uploaded.delimiter,
        canonicalInstrumentId,
      );
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
        mode: "single",
        report,
        declaration: JSON.stringify(selectedDeclaration),
      };
    },
    onSuccess: (result) => {
      if (result.mode === "batch") {
        setBatchItems(result.items);
        return;
      }
      setDryRun(result.report);
      setValidatedDeclaration(result.declaration);
    },
    onError: (value) => setError(message(value)),
  });

  const createImport = useMutation({
    mutationFn: async () => {
      if (batchItems.length) {
        if (
          inference
          && (
            ["low", "none"].includes(inference.timezone.confidence)
            || ["low", "none"].includes(inference.adjustment.confidence)
          )
          && !semanticsConfirmed
        ) {
          throw new Error("请先确认当前低置信度语义设置");
        }
        if (!batchItems.every((item) => (
          item.receipt
          && item.report?.status === "valid"
          && item.declaration
          && JSON.stringify(item.declaration) === JSON.stringify(
            buildDeclaration(
              columnMapping,
              delimiter,
              timezone,
              adjustment,
              item.externalSymbol,
              item.symbol,
            )
          )
        ))) {
          throw new Error("文件夹中的所有 CSV 必须先通过当前共享格式的预检");
        }
        let failures = 0;
        for (const item of batchItems) {
          if (item.jobId) {
            continue;
          }
          updateBatchItem(item.key, {
            stage: "creating", error: undefined, jobId: undefined,
          });
          try {
            const result = await api.request<{
              created: boolean;
              job: CsvJob;
            }>("/v1/admin/csv-imports", {
              method: "POST",
              body: JSON.stringify({
                upload_id: item.receipt?.upload_id,
                declaration: item.declaration,
                idempotency_key: `csv-upload-${item.receipt?.sha256}`,
                chunk_rows: 100000,
                max_attempts: 3,
              }),
            });
            updateBatchItem(item.key, {
              stage: "created",
              jobId: result.job.job_id,
            });
          } catch (value) {
            updateBatchItem(item.key, {
              stage: "failed",
              error: message(value),
            });
            if (authenticationFailed(value)) {
              throw value;
            }
            failures += 1;
          }
        }
        if (failures) {
          setError(`${failures} 个 CSV 创建任务失败；其余文件已继续处理`);
        }
        return { mode: "batch" as const };
      }

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
      ).then((result) => ({ mode: "single" as const, result }));
    },
    onSuccess: async (result) => {
      if (result.mode === "batch") {
        await queryClient.invalidateQueries({
          queryKey: ["csv-import-jobs"],
        });
        return;
      }
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
    setBatchItems([]);
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

  const chooseFolder = (event: ChangeEvent<HTMLInputElement>) => {
    const items = selectedCsvFiles(event.target.files);
    setFile(null);
    setBatchItems(items);
    setUpload(null);
    setDryRun(null);
    setValidatedDeclaration("");
    setInference(null);
    setInferenceKey("");
    setSemanticsConfirmed(false);
    setColumnMapping(defaultColumns);
    setDelimiter(",");
    setError(items.length ? "" : "所选文件夹中没有 CSV 文件");
  };

  const folderMode = batchItems.length > 0;
  const canonicalId = `${symbol.trim().toUpperCase()}.${mic}`;
  const batchCanonicalIds = batchItems.map(
    (item) => `${item.symbol}.${mic}`
  );
  const hasDuplicateBatchInstrument = (
    new Set(batchCanonicalIds).size !== batchCanonicalIds.length
  );
  const validForm = Boolean(
    source.trim()
    && mic
    && (
      folderMode
        ? batchItems.every((item) => (
          item.externalSymbol.trim() && item.symbol.trim()
        )) && !hasDuplicateBatchInstrument
        : file && externalSymbol.trim() && symbol.trim()
    ),
  );
  const batchDeclarationsMatch = folderMode && batchItems.every((item) => (
    item.receipt
    && item.report?.status === "valid"
    && item.declaration
    && JSON.stringify(item.declaration) === JSON.stringify(
      buildDeclaration(
        columnMapping,
        delimiter,
        timezone,
        adjustment,
        item.externalSymbol,
        item.symbol,
      )
    )
  ));
  const batchHasImportCandidate = batchItems.some((item) => !item.jobId);
  const batchPreflightMatches = (
    batchDeclarationsMatch && batchHasImportCandidate
  );
  const preflightMatches = folderMode
    ? batchPreflightMatches
    : dryRun?.status === "valid"
      && validatedDeclaration === JSON.stringify(buildDeclaration());
  const schemaColumns = upload?.columns
    ?? batchItems.find((item) => item.receipt)?.receipt?.columns;
  const batchReportsPassed = folderMode && batchItems.every(
    (item) => item.report?.status === "valid"
  );
  const batchHasFailure = folderMode && batchItems.some(
    (item) => item.stage === "invalid" || item.stage === "failed"
  );
  const displayedPreflightStatus = folderMode
    ? batchReportsPassed ? "valid" : batchHasFailure ? "invalid" : "pending"
    : dryRun?.status ?? "pending";
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
          文件或文件夹先进入本地受控暂存区，通过预检后才会创建可恢复的分片导入任务。
          MarketCow 内部标识始终使用 SYMBOL.MIC。
        </p>
      </section>

      <div className="csv-import-layout">
        <section className="data-card csv-upload-form">
          <header>
            <div><p className="eyebrow">01 / FILE</p><h3>文件与数据身份</h3></div>
            <span>CSV ONLY</span>
          </header>

          <div className="csv-source-picker">
            <label className={`file-drop ${file ? "has-file" : ""}`}>
              <input
                aria-label="选择单个 CSV 文件"
                type="file"
                accept=".csv,text/csv"
                onChange={chooseFile}
              />
              <strong>{file ? file.name : "选择单个 CSV 文件"}</strong>
              <span>
                {file
                  ? formatBytes(file.size)
                  : "使用独立的标的代码和格式设置"}
              </span>
            </label>
            <label className={`file-drop ${folderMode ? "has-file" : ""}`}>
              <input
                aria-label="选择 CSV 文件夹"
                type="file"
                accept=".csv,text/csv"
                multiple
                ref={(element) => {
                  if (element) {
                    element.setAttribute("webkitdirectory", "");
                    element.setAttribute("directory", "");
                  }
                }}
                onChange={chooseFolder}
              />
              <strong>
                {folderMode
                  ? `已选择 ${batchItems.length} 个 CSV`
                  : "选择一个 CSV 文件夹"}
              </strong>
              <span>共享一次格式设置，按路径顺序逐个处理</span>
            </label>
          </div>

          <div className="form-grid csv-column-grid">
            <label>数据供应商
              <input value={source} onChange={(event) => setSource(event.target.value)} />
            </label>
            {!folderMode ? (
              <>
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
              </>
            ) : null}
            <label>MIC
              <select aria-label="MIC" value={mic} onChange={(event) => setMic(event.target.value)}>
                {micOptions.map(([value, label]) => (
                  <option key={value} value={value}>{value} — {label}</option>
                ))}
              </select>
            </label>
          </div>

          {folderMode ? (
            <div className="identity-preview batch-identity-preview">
              <span>文件名映射预览</span>
              {batchItems.slice(0, 6).map((item) => (
                <small key={item.key}>
                  {item.path} · {item.externalSymbol} → {item.symbol}.{mic}
                </small>
              ))}
              {batchItems.length > 6 ? (
                <small>另有 {batchItems.length - 6} 个 CSV…</small>
              ) : null}
              {hasDuplicateBatchInstrument ? (
                <strong role="alert">文件名生成了重复 Instrument ID</strong>
              ) : null}
            </div>
          ) : (
            <div className="identity-preview">
              <span>统一 Instrument ID</span>
              <strong>{canonicalId}</strong>
              <small>{externalSymbol} → {canonicalId}</small>
            </div>
          )}

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
                {schemaColumns?.length ? (
                  <select
                    aria-label={`${field} 列`}
                    value={columnMapping[field]}
                    onChange={(event) => setColumnMapping({
                      ...columnMapping, [field]: event.target.value,
                    })}
                  >
                    {schemaColumns.map((column) => (
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
            时间、OHLC 和成交量映射。{folderMode
              ? "文件夹模式会把第一个 CSV 的格式和语义设置应用到全部文件。"
              : "当前文件按单一标的导入，因此标的代码不必作为 CSV 列存在。"}
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
              {preflight.isPending
                ? folderMode ? "正在依次检查…" : "上传并检查中…"
                : folderMode ? "批量上传并预检" : "上传并预检"}
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
              {createImport.isPending
                ? folderMode ? "正在依次创建…" : "正在创建…"
                : folderMode ? "开始批量导入" : "开始正式导入"}
            </button>
          </div>
        </section>

        <aside className="data-card csv-preflight">
          <header>
            <div><p className="eyebrow">PREFLIGHT</p><h3>预检结果</h3></div>
            <span className={`status-pill status-${displayedPreflightStatus}`}>
              {displayedPreflightStatus === "pending"
                ? "等待"
                : displayedPreflightStatus}
            </span>
          </header>
          {folderMode ? (
            <div className="batch-preflight">
              <p>
                {batchItems.filter(
                  (item) => item.report?.status === "valid"
                ).length}
                {" / "}
                {batchItems.length} 通过
              </p>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>CSV</th><th>映射</th><th>状态</th><th>有效行</th>
                    </tr>
                  </thead>
                  <tbody>
                    {batchItems.map((item) => (
                      <tr key={item.key}>
                        <td title={item.path}>{item.path}</td>
                        <td>{item.symbol}.{mic}</td>
                        <td>
                          <span className={`batch-stage batch-stage-${item.stage}`}>
                            {batchStageLabels[item.stage]}
                          </span>
                          {item.jobId ? (
                            <small title={item.jobId}>
                              {item.jobId.slice(0, 10)}
                            </small>
                          ) : null}
                          {item.error ? (
                            <small className="batch-item-error">{item.error}</small>
                          ) : null}
                        </td>
                        <td>{formatRows(item.report?.rows_valid)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ) : upload ? (
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
                    <div>
                      <progress
                        aria-label={`${job.job_id} 导入进度`}
                        max="100"
                        value={job.progress_percent ?? 0}
                      />
                      <span>{job.progress_percent ?? 0}%</span>
                    </div>
                    <small>{phaseLabel(job)}</small>
                    {isStalled(job) ? (
                      <strong role="alert">处理可能停滞</strong>
                    ) : null}
                  </td>
                  <td>
                    {formatRows(job.rows_written)} / {formatRows(job.rows_total)}
                  </td>
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
