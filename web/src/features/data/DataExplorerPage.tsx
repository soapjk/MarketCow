import { useState, type FormEvent } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "../../lib/api";

type Instrument = {
  instrument_id?: string; symbol: string; name?: string; market?: string;
  exchange?: string; currency?: string; source?: string;
};
type SearchResult = { count: number; items: Instrument[] };
type Coverage = {
  symbol: string;
  summary: { layers: string[]; intervals: string[]; rows: number; sources: string[] };
  items: { layer: string; interval: string; adjustment: string; first_bar: string; last_bar: string; row_count: number; sources: string[] }[];
};

export function DataExplorerPage() {
  const [input, setInput] = useState("");
  const [queryText, setQueryText] = useState("");
  const [symbol, setSymbol] = useState("");
  const search = useQuery({
    queryKey: ["instrument-search", queryText],
    queryFn: () => api.request<SearchResult>(`/v1/instruments/search?q=${encodeURIComponent(queryText)}&limit=20`),
    enabled: Boolean(queryText),
  });
  const coverage = useQuery({
    queryKey: ["instrument-coverage", symbol],
    queryFn: () => api.request<Coverage>(`/v1/admin/instruments/${encodeURIComponent(symbol)}/coverage`),
    enabled: Boolean(symbol),
  });
  function submit(event: FormEvent) {
    event.preventDefault();
    setQueryText(input.trim());
    setSymbol("");
  }
  return (
    <section>
      <div className="page-intro"><div><p className="eyebrow">INSTRUMENTS / CANONICAL</p><h2>数据浏览</h2></div><p>搜索标的并核对 ClickHouse raw/canonical 覆盖范围与来源。</p></div>
      <form className="search-bar" onSubmit={submit}><input aria-label="搜索标的" placeholder="代码、名称或 instrument ID" value={input} onChange={(e) => setInput(e.target.value)} /><button type="submit">搜索</button></form>
      <div className="explorer-grid">
        <article className="data-card">
          <header><div><p className="eyebrow">SEARCH RESULTS</p><h3>标的</h3></div><span>{search.data?.count ?? 0} 项</span></header>
          {!queryText ? <p className="empty-copy">输入代码或名称开始搜索。</p>
            : search.isPending ? <p className="empty-copy">搜索中…</p>
            : search.isError ? <p className="inline-error">{search.error.message}</p>
            : <div className="instrument-list">{search.data.items.map((item) => (
              <button className={symbol === item.symbol ? "selected" : ""} key={`${item.source}-${item.symbol}`} onClick={() => setSymbol(item.symbol)}>
                <div><strong>{item.symbol}</strong><span>{item.name || "未命名标的"}</span></div><small>{item.market || item.exchange || "—"} / {item.source || "master"}</small>
              </button>
            ))}</div>}
        </article>
        <article className="data-card">
          <header><div><p className="eyebrow">STORAGE COVERAGE</p><h3>{symbol || "选择一个标的"}</h3></div></header>
          {!symbol ? <p className="empty-copy">选择左侧结果查看存储覆盖。</p>
            : coverage.isPending ? <p className="empty-copy">正在查询 ClickHouse…</p>
            : coverage.isError ? <p className="inline-error">{coverage.error.message}</p>
            : <>
              <div className="coverage-stats"><div><span>Rows</span><strong>{coverage.data.summary.rows.toLocaleString()}</strong></div><div><span>Layers</span><strong>{coverage.data.summary.layers.join(", ") || "—"}</strong></div><div><span>Intervals</span><strong>{coverage.data.summary.intervals.join(", ") || "—"}</strong></div><div><span>Sources</span><strong>{coverage.data.summary.sources.join(", ") || "—"}</strong></div></div>
              <div className="table-wrap"><table><thead><tr><th>层</th><th>周期</th><th>复权</th><th>Rows</th><th>首条</th><th>末条</th></tr></thead><tbody>{coverage.data.items.map((item) => <tr key={`${item.layer}-${item.interval}-${item.adjustment}`}><td>{item.layer}</td><td>{item.interval}</td><td>{item.adjustment}</td><td>{item.row_count.toLocaleString()}</td><td>{new Date(item.first_bar).toLocaleString()}</td><td>{new Date(item.last_bar).toLocaleString()}</td></tr>)}</tbody></table></div>
            </>}
        </article>
      </div>
    </section>
  );
}
