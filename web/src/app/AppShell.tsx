import { useEffect, useState, type ReactNode } from "react";
import { useIdentity } from "../features/auth/authContext";

const navigation = [
  ["overview", "总览", "01"],
  ["dashboards", "数据看板", "02"],
  ["data", "数据浏览", "03"],
  ["operations", "任务与服务", "04"],
  ["live", "实时监控", "05"],
  ["settings", "管理设置", "06"],
] as const;

const titles = Object.fromEntries(navigation.map(([path, title]) => [`/${path}`, title]));

export function AppShell({ path, children }: { path: string; children: ReactNode }) {
  const identity = useIdentity();
  const [theme, setTheme] = useState<"dark" | "light">(() => {
    return localStorage.getItem("marketcow-theme") === "light" ? "light" : "dark";
  });

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem("marketcow-theme", theme);
  }, [theme]);

  const title = titles[path] ?? "页面";

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark" aria-hidden="true">M</div>
          <div>
            <strong>MarketCow</strong>
            <span>CONTROL / LOCAL</span>
          </div>
        </div>

        <nav aria-label="主导航">
          {navigation.map(([route, label, index]) => (
            <a key={route} href={`#/${route}`} className={`/${route}` === path ? "active" : ""}>
              <span className="nav-index">{index}</span>
              <span>{label}</span>
            </a>
          ))}
        </nav>

        <div className="system-badge">
          <span className="pulse-dot" />
          <div><strong>LOCAL RUNTIME</strong><span>127.0.0.1</span></div>
        </div>
      </aside>

      <div className="workspace">
        <header className="topbar">
          <div>
            <span className="breadcrumb">MARKETCOW / {title.toUpperCase()}</span>
            <h1>{title}</h1>
          </div>
          <div className="topbar-actions">
            <label className="project-switcher">
              <span>项目</span>
              <select aria-label="当前项目" defaultValue="marketcow">
                <option value="marketcow">MarketCow</option>
              </select>
            </label>
            <button
              className="icon-button"
              type="button"
              aria-label={`切换到${theme === "dark" ? "浅色" : "深色"}主题`}
              onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
            >
              {theme === "dark" ? "☼" : "◐"}
            </button>
            <button className="identity-button" type="button" onClick={identity?.logout} title="退出当前会话">
              <span>{identity?.role ?? "local"}</span>
              {identity?.actor ?? "development"}
            </button>
          </div>
        </header>
        <main className="content">{children}</main>
      </div>
    </div>
  );
}
