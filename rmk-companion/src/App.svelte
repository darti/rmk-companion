<script lang="ts">
  import Editor from './lib/Editor.svelte';
  import { invoke } from '@tauri-apps/api/tauri';

  function query() {
    invoke('run_query', { query: 'Hello!' });
  }
</script>

<main class="flex flex-col">
  <Editor />
  <button on:click={query}>Run</button>
</main>

<style>
  :root {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen,
      Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;
  }

  main {
    padding: 0;
    margin: 0;

    height: 100vh;
    width: 100vw;
  }
</style>
