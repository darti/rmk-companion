<script lang="ts">
  import Editor from './lib/Editor.svelte';
</script>

<main class="flex flex-col">
  <Editor />
</main>

<style>
  :root {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen,
      Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;
  }

  main {
    text-align: center;
    padding: 1em;
    margin: 0;

    height: 100vh;
    width: 100vw;
  }
</style>
