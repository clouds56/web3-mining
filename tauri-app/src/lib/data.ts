import { EChartsOption } from "echarts"

export interface Data {
  name: string
  data: { [name: string]: (number | null)[] }
  time: (number | null)[]
}

export function lineOption(data: Data, col: string | number): EChartsOption {
  return {
    tooltip: {
      trigger: "axis",
    },
    dataset: {
      source: data.data,
    },
    xAxis: {
      type: "time",
      // @ts-ignore: this is a bug in the types
      data: data.time,
    },
    yAxis: { type: "value" },
    series: {
      type: "line",
      encode: { y: col },
    },
  }
}
