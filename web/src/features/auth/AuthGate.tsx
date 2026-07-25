import { useState, type FormEvent, type ReactNode } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, ApiError } from "../../lib/api";
import { AuthContext, type Identity } from "./authContext";

export function AuthGate({ children }: { children: ReactNode }) {
  const client = useQueryClient();
  const [token, setToken] = useState("");
  const session = useQuery({
    queryKey: ["admin-session"],
    queryFn: () => api.request<Identity>("/v1/auth/session"),
    retry: false,
  });
  const login = useMutation({
    mutationFn: () => api.request<Identity>("/v1/auth/session", {
      method: "POST", body: JSON.stringify({ token }),
    }),
    onSuccess: (identity) => client.setQueryData(["admin-session"], identity),
  });
  const logout = useMutation({
    mutationFn: () => api.request<void>("/v1/auth/session", { method: "DELETE" }),
    onSettled: () => {
      client.removeQueries();
      window.location.reload();
    },
  });
  function submit(event: FormEvent) {
    event.preventDefault();
    login.mutate();
  }
  if (session.isPending) {
    return <main className="auth-screen"><div className="auth-panel"><p className="eyebrow">MARKETCOW CONTROL</p><h1>正在验证本地会话</h1></div></main>;
  }
  if (session.isError) {
    const needsLogin = session.error instanceof ApiError && session.error.status === 401;
    if (!needsLogin) {
      return <main className="auth-screen"><div className="auth-panel"><p className="eyebrow">CONNECTION ERROR</p><h1>无法连接控制面</h1><p>{session.error.message}</p><button onClick={() => session.refetch()}>重试</button></div></main>;
    }
    return (
      <main className="auth-screen">
        <form className="auth-panel" onSubmit={submit}>
          <div className="brand-mark">M</div>
          <p className="eyebrow">LOCAL ADMINISTRATION</p>
          <h1>进入 MarketCow Control</h1>
          <p>输入本机配置的 Viewer、Operator 或 Admin bootstrap token。</p>
          <label>访问令牌<input autoFocus type="password" autoComplete="current-password" value={token} onChange={(event) => setToken(event.target.value)} required /></label>
          <button type="submit" disabled={login.isPending}>{login.isPending ? "验证中…" : "登录"}</button>
          {login.isError && <p className="inline-error">{login.error.message}</p>}
        </form>
      </main>
    );
  }
  return (
    <AuthContext.Provider value={{ ...session.data, logout: () => logout.mutate() }}>
      {children}
    </AuthContext.Provider>
  );
}
