import "@testing-library/jest-dom/vitest";

Object.defineProperty(globalThis, "crypto", {
  value: { ...globalThis.crypto, randomUUID: () => "00000000-0000-4000-8000-000000000000" },
  configurable: true,
});
