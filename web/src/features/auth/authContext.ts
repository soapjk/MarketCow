import { createContext, useContext } from "react";

export type Identity = {
  authenticated: true;
  actor: string;
  role: "viewer" | "operator" | "admin";
};

export type AuthContextValue = Identity & { logout: () => void };
export const AuthContext = createContext<AuthContextValue | null>(null);

export function useIdentity() {
  return useContext(AuthContext);
}
