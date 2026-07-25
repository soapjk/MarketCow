import { useMemo, useState } from "react";
import type { EChartsCoreOption } from "echarts/core";
import { EChart } from "../../components/EChart";
import { useLiveRequests } from "./useLiveRequests";

const colors = ["#c6ff4a", "#4de3ff", "#f5b942", "#ff7171", "#ad8cff"];

export function LiveMonitorPage() {
  const [paused, setPaused] = useState(false);
  const { state, snapshot, clear } = useLiveRequests(paused);
  const labels = snapshot.timeline.map((item) => new Date(item.at).toLocaleTimeString());

  const rateOption = useMemo<EChartsCoreOption>(() => ({
    animation: false,
    color: colors,
    tooltip: { trigger: "axis" },
    grid: { left: 46, right: 16, top: 24, bottom: 30 },
    xAxis: { type: "category", data: labels, axisLabel: { color: "#8293a8", interval: 9 } },
    yAxis: { type: "value", minInterval: 1, axisLabel: { color: "#8293a8" }, splitLine: { lineStyle: { color: "#21344c" } } },
    series: [{ name: "requests/s", type: "line", showSymbol: false, areaStyle: { opacity: .12 }, data: snapshot.timeline.map((item) => item.requests) }],
  }), [labels, snapshot.timeline]);

  const latencyOption = useMemo<EChartsCoreOption>(() => ({
    animation: false,
    color: [colors[1], colors[2]],
    tooltip: { trigger: "axis" },
    legend: { textStyle: { color: "#8293a8" } },
    grid: { left: 52, right: 16, top: 34, bottom: 30 },
    xAxis: { type: "category", data: labels, axisLabel: { color: "#8293a8", interval: 9 } },
    yAxis: { type: "value", name: "ms", axisLabel: { color: "#8293a8" }, splitLine: { lineStyle: { color: "#21344c" } } },
    series: [
      { name: "P50", type: "line", showSymbol: false, data: snapshot.timeline.map((item) => item.p50) },
      { name: "P95", type: "line", showSymbol: false, data: snapshot.timeline.map((item) => item.p95) },
    ],
  }), [labels, snapshot.timeline]);

  const statusOption = useMemo<EChartsCoreOption>(() => ({
    animationDurationUpdate: 150,
    color: colors,
    tooltip: { trigger: "item" },
    legend: { bottom: 0, textStyle: { color: "#8293a8" } },
    series: [{
      type: "pie", radius: ["48%", "72%"], center: ["50%", "45%"],
      label: { color: "#e7edf5" }, data: snapshot.statuses,
    }],
  }), [snapshot.statuses]);

  return (
    <section className="live-page">
      <div className="page-intro">
        <div><p className="eyebrow">SUB-SECOND / SSE / ECHARTS</p><h2>实时请求监控</h2></div>
        <div className="live-controls">
          <span className={`connection-state state-${state}`}>{state}</span>
          <button type="button" onClick={() => setPaused(!paused)}>{paused ? "继续" : "暂停"}</button>
          <button type="button" className="secondary" onClick={clear}>清空窗口</button>
        </div>
      </div>
      <div className="live-stats">
        <div><span>60 秒请求</span><strong>{snapshot.total}</strong></div>
        <div><span>当前在途</span><strong>{snapshot.inFlight}</strong></div>
        <div><span>最近错误</span><strong>{snapshot.errors.length}</strong></div>
        <div><span>缓冲策略</span><strong>2K / 250ms</strong></div>
      </div>
      <div className="chart-grid">
        <article className="chart-card wide"><header><h3>请求速率</h3><span>rolling 60s</span></header><EChart option={rateOption} ariaLabel="最近六十秒请求速率折线图" /></article>
        <article className="chart-card"><header><h3>状态分布</h3><span>status family</span></header><EChart option={statusOption} ariaLabel="最近六十秒请求状态分布环形图" /></article>
        <article className="chart-card wide"><header><h3>响应延迟</h3><span>client window</span></header><EChart option={latencyOption} ariaLabel="最近六十秒响应延迟分位数折线图" /></article>
        <article className="chart-card error-feed">
          <header><h3>最新错误</h3><span>bounded 20</span></header>
          {snapshot.errors.length ? snapshot.errors.map((event) => (
            <div className="error-row" key={event.event_id}>
              <span>{new Date(event.occurred_at).toLocaleTimeString()}</span>
              <strong>{event.payload.status_family}</strong>
              <code>{event.payload.route}</code>
            </div>
          )) : <p className="empty-copy">当前窗口没有服务端错误。</p>}
        </article>
      </div>
    </section>
  );
}
