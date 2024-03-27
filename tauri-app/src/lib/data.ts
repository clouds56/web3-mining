import { EChartsOption } from "echarts"

export function lineOption(data: any): EChartsOption {
  return {
    tooltip: {
      trigger: "axis",
    },
    dataset: {
      // id: "test_data",
      dimensions: ["time", "value"],
      source: data,
    },
    xAxis: { type: "time" },
    yAxis: { type: "value" },
    series: {
      type: "line",
    },
  }
}
