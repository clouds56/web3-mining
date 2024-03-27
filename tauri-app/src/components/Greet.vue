<script setup lang="ts">
import { ref } from "vue"
import { invoke } from "@tauri-apps/api/tauri"

const data_names = ref<string[]>([])
const name = ref("")

const select = (n: string) => {
  name.value = n
}

async function greet() {
  // Learn more about Tauri commands at https://tauri.app/v1/guides/features/command
  data_names.value = await invoke("list_data_names", { name: name.value })
}
greet()
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
      <div class="flex flex-row grow bg-gray-500"></div>
    </div>
  </div>
</template>

<style scoped>
#greet-input {
  margin-right: 5px;
}
</style>
