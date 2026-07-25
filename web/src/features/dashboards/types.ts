export type DashboardRegistration = {
  key: string;
  project: string;
  name: string;
  description: string;
  dashboard_uid: string;
  panel_id: number | null;
  theme: "current" | "dark" | "light";
  sort_order: number;
  path: string;
};

export type DashboardRegistryDocument = {
  schema: "marketcow.dashboard-registry.v1";
  items: DashboardRegistration[];
};
