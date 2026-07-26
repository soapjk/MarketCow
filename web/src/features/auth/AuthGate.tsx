import { useEffect, useState, type FormEvent, type ReactNode } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  api,
  ApiError,
  AUTHENTICATION_REQUIRED_EVENT,
} from "../../lib/api";
import { AuthContext, type Identity } from "./authContext";

export function AuthGate({ children }: { children: ReactNode }) {
  const client = useQueryClient();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const session = useQuery({
    queryKey: ["admin-session"],
    queryFn: () => api.request<Identity>("/v1/auth/session"),
    retry: false,
    refetchInterval: 60_000,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: true,
  });
  useEffect(() => {
    const revalidateSession = () => {
      void client.resetQueries({
        queryKey: ["admin-session"],
        exact: true,
      });
    };
    window.addEventListener(
      AUTHENTICATION_REQUIRED_EVENT,
      revalidateSession,
    );
    return () => window.removeEventListener(
      AUTHENTICATION_REQUIRED_EVENT,
      revalidateSession,
    );
  }, [client]);
  const login = useMutation({
    mutationFn: () => api.request<Identity>("/v1/auth/session", {
      method: "POST", body: JSON.stringify({ username, password }),
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
          <p>使用本机配置的管理账户登录。程序调用仍可使用 bootstrap token。</p>
          <label>用户名<input autoFocus type="text" autoComplete="username" value={username} onChange={(event) => setUsername(event.target.value)} required /></label>
          <label>密码<input type="password" autoComplete="current-password" value={password} onChange={(event) => setPassword(event.target.value)} required /></label>
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
