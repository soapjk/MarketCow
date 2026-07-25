import { useEffect, useState } from "react";
import type { DashboardRegistration } from "./types";

function grafanaUrl(path: string) {
  const base = (import.meta.env.VITE_GRAFANA_BASE_URL || "http://127.0.0.1:3001").replace(/\/$/, "");
  return `${base}${path}`;
}

export function GrafanaFrame({ dashboard }: { dashboard: DashboardRegistration }) {
  const [loaded, setLoaded] = useState(false);
  const [timedOut, setTimedOut] = useState(false);
  const src = grafanaUrl(dashboard.path);

  useEffect(() => {
    const timeout = window.setTimeout(() => {
      if (!loaded) setTimedOut(true);
    }, 12_000);
    return () => window.clearTimeout(timeout);
  }, [loaded, src]);

  return (
    <article className="grafana-card">
      <header>
        <div>
          <span>{dashboard.project}</span>
          <h3>{dashboard.name}</h3>
          <p>{dashboard.description}</p>
        </div>
        <a href={src} target="_blank" rel="noreferrer">在 Grafana 打开 ↗</a>
      </header>
      <div className="grafana-frame-wrap">
        {!loaded && !timedOut && <div className="frame-status">正在连接 Grafana…</div>}
        {timedOut && (
          <div className="frame-status frame-warning">
            <strong>看板未能完成加载</strong>
            <span>Grafana 可能未启动、登录已失效，或嵌入尚未启用。</span>
            <a href={src} target="_blank" rel="noreferrer">直接打开并登录</a>
          </div>
        )}
        <iframe
          title={`${dashboard.project} - ${dashboard.name}`}
          src={src}
          loading="lazy"
          onLoad={() => setLoaded(true)}
          onError={() => setTimedOut(true)}
          referrerPolicy="same-origin"
          allow="fullscreen"
        />
      </div>
    </article>
  );
}
