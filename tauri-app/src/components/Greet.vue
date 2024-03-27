<script setup lang="ts">
import { ref, provide, shallowRef, watch } from "vue"
import { invoke } from "@tauri-apps/api/tauri"

// tauri

const data_names = ref<string[]>([])
const name = ref("")
const data = shallowRef<any>(null)

const select = (n: string) => {
  name.value = n
}

const greet = async () => {
  // Learn more about Tauri commands at https://tauri.app/v1/guides/features/command
  data_names.value = await invoke("list_data_names")
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
import { lineOption } from "../lib/data"
const echart_data = ref<EChartsOption>({})
watch(data, (new_data) => {
  if (!new_data) return
  echart_data.value = lineOption(new_data)
})
</script>

<template>
  <div class="flex flex-col w-full h-full">
    <!-- top panel -->
    <form class="flex flex-row justify-center mb-1 select-none" @submit.prevent="greet">
      <input id="greet-input" v-model="name" placeholder="Enter a name..." />
      <button type="submit">Greet</button>
    </form>

    <div class="flex flex-row grow">
      <!--  left panel -->
      <ul class="flex flex-row basis-[200px] select-none">
        <li v-for="n in data_names">
          <label>
            <input type="radio" :name="n" @click="select(n)" />
            <span class="ml-1">{{ n }}</span>
          </label>
        </li>
      </ul>
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
