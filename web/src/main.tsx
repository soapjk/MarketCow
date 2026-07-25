import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { QueryClientProvider } from "@tanstack/react-query";
import { App } from "./app/App";
import { queryClient } from "./lib/queryClient";
import "./styles.css";
import { AuthGate } from "./features/auth/AuthGate";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <AuthGate><App /></AuthGate>
    </QueryClientProvider>
  </StrictMode>,
);
