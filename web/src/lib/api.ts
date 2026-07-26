export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly requestId?: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

export const AUTHENTICATION_REQUIRED_EVENT =
  "marketcow:authentication-required";

export type ApiClientOptions = {
  baseUrl?: string;
  fetchImpl?: typeof fetch;
};

function errorMessage(body: unknown, fallback: string) {
  if (!body || typeof body !== "object") return fallback;
  const { detail, message } = body as {
    detail?: unknown;
    message?: unknown;
  };
  if (typeof detail === "string") return detail;
  if (detail && typeof detail === "object") {
    const { code } = detail as { code?: unknown };
    if (code === "authentication_required") {
      return "登录会话已失效，请重新登录";
    }
    if (typeof code === "string") return code;
  }
  return typeof message === "string" ? message : fallback;
}

function cookie(name: string) {
  const prefix = `${encodeURIComponent(name)}=`;
  const item = document.cookie.split(";").map((value) => value.trim())
    .find((value) => value.startsWith(prefix));
  return item ? decodeURIComponent(item.slice(prefix.length)) : "";
}

export function createApiClient(options: ApiClientOptions = {}) {
  const baseUrl = (options.baseUrl ?? import.meta.env.VITE_API_BASE_URL ?? "").replace(/\/$/, "");
  const fetchImpl = options.fetchImpl;

  async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
    const method = (init.method ?? "GET").toUpperCase();
    const mutating = !["GET", "HEAD", "OPTIONS"].includes(method);
    const csrf = mutating ? cookie("marketcow_csrf") : "";
    const response = await (fetchImpl ?? fetch)(`${baseUrl}${path}`, {
      ...init,
      headers: {
        Accept: "application/json",
        ...(init.body ? { "Content-Type": "application/json" } : {}),
        ...(mutating ? { "X-Request-ID": crypto.randomUUID() } : {}),
        ...(csrf ? { "X-CSRF-Token": csrf } : {}),
        ...init.headers,
      },
      credentials: "same-origin",
    });
    if (!response.ok) {
      let message = `请求失败 (${response.status})`;
      try {
        message = errorMessage(await response.json(), message);
      } catch {
        // Preserve the status-based fallback for non-JSON failures.
      }
      if (
        response.status === 401
        && path !== "/v1/auth/session"
        && typeof window !== "undefined"
      ) {
        window.dispatchEvent(new Event(AUTHENTICATION_REQUIRED_EVENT));
      }
      throw new ApiError(message, response.status, response.headers.get("x-request-id") ?? undefined);
    }
    if (response.status === 204) return undefined as T;
    return response.json() as Promise<T>;
  }

  return { request };
}

export const api = createApiClient();
