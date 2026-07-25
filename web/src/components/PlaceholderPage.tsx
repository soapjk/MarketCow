const content = {
  overview: ["统一运行视图", "系统健康、数据新鲜度、任务和实时请求将在这里汇合。"],
  dashboards: ["Grafana 看板入口", "注册后的 MarketCow 与其他本地项目看板将在这里显示。"],
  data: ["数据浏览", "搜索标的并检查 raw、canonical、覆盖范围和 Artifact。"],
  operations: ["任务与服务", "管理历史任务、Provider 状态和受保护的操作。"],
  live: ["实时监控", "通过 WebSocket 或 SSE 接收亚秒级摘要并交给 ECharts。"],
  settings: ["管理设置", "配置看板注册、访问策略和审计查询。"],
  "not-found": ["页面不存在", "当前地址没有对应的管理页面。"],
} as const;

export function PlaceholderPage({ kind }: { kind: keyof typeof content }) {
  const [title, description] = content[kind];
  return (
    <section className="placeholder-page">
      <div className="status-strip">
        <span>BUILDING</span><span>VISUALIZATION MODULE</span><span>LOCAL ONLY</span>
      </div>
      <div className="hero-card">
        <p className="eyebrow">MARKETCOW ADMINISTRATION</p>
        <h2>{title}</h2>
        <p>{description}</p>
        <div className="skeleton-grid" aria-label="内容正在建设">
          <div /><div /><div />
        </div>
      </div>
    </section>
  );
}
