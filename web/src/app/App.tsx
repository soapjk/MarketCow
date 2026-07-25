import {
  Component, Suspense, lazy, type ErrorInfo, type ReactNode, useEffect, useState,
} from "react";
import { AppShell } from "./AppShell";
import { PlaceholderPage } from "../components/PlaceholderPage";
import { DashboardsPage } from "../features/dashboards/DashboardsPage";
import { OverviewPage } from "../features/overview/OverviewPage";
import { OperationsPage } from "../features/operations/OperationsPage";
import { DataExplorerPage } from "../features/data/DataExplorerPage";
import { SettingsPage } from "../features/settings/SettingsPage";

const LiveMonitorPage = lazy(async () => {
  const module = await import("../features/live/LiveMonitorPage");
  return { default: module.LiveMonitorPage };
});

type ErrorBoundaryState = { error: Error | null };

class ErrorBoundary extends Component<{ children: ReactNode }, ErrorBoundaryState> {
  state: ErrorBoundaryState = { error: null };

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error("Uncaught administration UI error", error, info);
  }

  render() {
    if (this.state.error) {
      return (
        <main className="fatal-error">
          <p className="eyebrow">页面发生错误</p>
          <h1>控制台暂时无法显示</h1>
          <p>{this.state.error.message}</p>
          <button type="button" onClick={() => window.location.reload()}>
            重新加载
          </button>
        </main>
      );
    }
    return this.props.children;
  }
}

export function App() {
  const [path, setPath] = useState(() => window.location.hash.slice(1) || "/overview");

  useEffect(() => {
    if (!window.location.hash) window.location.replace("#/overview");
    const onHashChange = () => setPath(window.location.hash.slice(1) || "/overview");
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);

  const [route, subroute] = path.slice(1).split("/");
  const kind = route === "overview" || route === "dashboards" || route === "data"
    || route === "operations" || route === "live" || route === "settings"
    ? route
    : "not-found";
  const shellPath = kind === "not-found" ? path : `/${kind}`;

  return (
    <ErrorBoundary>
      <AppShell path={shellPath}>
        {kind === "overview" ? <OverviewPage />
          : kind === "dashboards" ? <DashboardsPage />
          : kind === "data" ? <DataExplorerPage />
          : kind === "operations" ? <OperationsPage initialTab={subroute === "providers" ? "providers" : "jobs"} />
          : kind === "live" ? (
            <Suspense fallback={<section className="page-state">正在加载实时图表引擎…</section>}>
              <LiveMonitorPage />
            </Suspense>
          ) : kind === "settings" ? <SettingsPage />
          : <PlaceholderPage kind={kind} />}
      </AppShell>
    </ErrorBoundary>
  );
}
