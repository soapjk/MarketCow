import { Component, type ErrorInfo, type ReactNode, useEffect, useState } from "react";
import { AppShell } from "./AppShell";
import { PlaceholderPage } from "../components/PlaceholderPage";

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

  const route = path.slice(1);
  const kind = route === "overview" || route === "dashboards" || route === "data"
    || route === "operations" || route === "live" || route === "settings"
    ? route
    : "not-found";

  return (
    <ErrorBoundary>
      <AppShell path={path}>
        <PlaceholderPage kind={kind} />
      </AppShell>
    </ErrorBoundary>
  );
}
