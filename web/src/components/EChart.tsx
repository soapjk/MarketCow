import { useEffect, useRef } from "react";
import * as echarts from "echarts/core";
import { LineChart, PieChart, CandlestickChart, BarChart } from "echarts/charts";
import {
  GridComponent, LegendComponent, TooltipComponent, DatasetComponent,
  DataZoomComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import type { EChartsCoreOption } from "echarts/core";

echarts.use([
  LineChart, PieChart, CandlestickChart, BarChart,
  GridComponent, LegendComponent, TooltipComponent, DatasetComponent,
  DataZoomComponent, CanvasRenderer,
]);

export function EChart({
  option,
  className = "",
  ariaLabel,
}: {
  option: EChartsCoreOption;
  className?: string;
  ariaLabel: string;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const chart = useRef<echarts.EChartsType | null>(null);

  useEffect(() => {
    if (!ref.current) return;
    chart.current = echarts.init(ref.current, undefined, { renderer: "canvas" });
    const observer = new ResizeObserver(() => chart.current?.resize());
    observer.observe(ref.current);
    return () => {
      observer.disconnect();
      chart.current?.dispose();
      chart.current = null;
    };
  }, []);

  useEffect(() => {
    chart.current?.setOption(option, { notMerge: false, lazyUpdate: true });
  }, [option]);

  return <div ref={ref} className={`echart ${className}`} role="img" aria-label={ariaLabel} />;
}
