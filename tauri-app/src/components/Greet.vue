<script setup lang="ts">
import { ref, provide, shallowRef, watch, computed } from "vue"
import { invoke } from "@tauri-apps/api/tauri"
import { lineOption, type Data } from "../lib/data"

// tauri
interface DatasetInfo {
  name: string
  collection: string[]
  max: number
}

const dataset_infos = ref<DatasetInfo[]>([])
const name = ref("")
const data = shallowRef<Data | null>(null)
const selected_col = shallowRef<string>("")
const data_key = computed(() => Object.keys(data.value?.data ?? {}))

const select = (n: string) => {
  name.value = n
}
const select_col = (n: string) => {
  selected_col.value = n
}

const greet = async () => {
  // Learn more about Tauri commands at https://tauri.app/v1/guides/features/command
  dataset_infos.value = await invoke("list_data_names")
}

const fetch_data = async () => {
  if (!name.value) return
  console.log(name.value)
  data.value = await invoke("get_data", { name: name.value })
}

watch(name, fetch_data, { immediate: true })

greet()

// echart
import { use } from "echarts/core"
import { CanvasRenderer } from "echarts/renderers"
import { PieChart, LineChart } from "echarts/charts"
import {
  TitleComponent,
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DatasetComponent,
} from "echarts/components"
import type { EChartsOption } from "echarts"
import VChart, { THEME_KEY } from "vue-echarts"

use([
  CanvasRenderer,
  PieChart,
  LineChart,
  TitleComponent,
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DatasetComponent,
])

provide(THEME_KEY, "dark")

const echart_data = ref<EChartsOption>({})
watch([data, selected_col], ([new_data, col]) => {
  if (!new_data || !col) return
  echart_data.value = lineOption(new_data, col)
})
</script>

<template>
  <div class="flex flex-col w-full h-full">
    <!-- top panel -->
    <form class="flex flex-row justify-center mb-1 select-none" @submit.prevent="greet">
      <input id="greet-input" v-model="name" placeholder="Enter a name..." />
      <button type="submit">Greet</button>
    </form>

    <div class="flex flex-row flex-auto">
      <!--  left panel -->
      <div class="flex flex-col basis-[200px] grow-0">
        <ul class="flex flex-col basis-[100px] flex-auto select-none overflow-scroll">
          <li v-for="info in dataset_infos">
            <label>
              <input type="radio" :name="info.name" @click="select(info.name)" />
              <span class="ml-1">{{ info.name }}[{{ info.max }}]</span>
            </label>
          </li>
        </ul>
        <form class="flex flex-col basis-[50px] flex-auto overflow-scroll">
          <label v-for="col in data_key">
            <input
              type="radio"
              :name="col"
              @click="select_col(col)"
              :checked="col == selected_col"
            />
            <pre class="ml-1 inline-block">{{ col }}</pre>
          </label>
        </form>
      </div>
      <!-- main panel -->
      <div class="flex flex-row grow bg-gray-500">
        <v-chart class="chart" :option="echart_data" autoresize />
      </div>
    </div>
  </div>
</template>

<style scoped>
#greet-input {
  margin-right: 5px;
}
</style>
