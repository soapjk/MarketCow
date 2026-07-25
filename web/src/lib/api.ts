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

export type ApiClientOptions = {
  baseUrl?: string;
  fetchImpl?: typeof fetch;
};

export function createApiClient(options: ApiClientOptions = {}) {
  const baseUrl = (options.baseUrl ?? import.meta.env.VITE_API_BASE_URL ?? "").replace(/\/$/, "");
  const fetchImpl = options.fetchImpl ?? fetch;

  async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
    const response = await fetchImpl(`${baseUrl}${path}`, {
      ...init,
      headers: {
        Accept: "application/json",
        ...(init.body ? { "Content-Type": "application/json" } : {}),
        ...init.headers,
      },
      credentials: "same-origin",
    });
    if (!response.ok) {
      let message = `请求失败 (${response.status})`;
      try {
        const body = await response.json() as { detail?: string; message?: string };
        message = body.detail ?? body.message ?? message;
      } catch {
        // Preserve the status-based fallback for non-JSON failures.
      }
      throw new ApiError(message, response.status, response.headers.get("x-request-id") ?? undefined);
    }
    if (response.status === 204) return undefined as T;
    return response.json() as Promise<T>;
  }

  return { request };
}

export const api = createApiClient();
