<script lang="ts">
  import { onMount } from 'svelte';

  import * as monaco from 'monaco-editor';
  import editorWorker from 'monaco-editor/esm/vs/editor/editor.worker?worker';

  let container;

  onMount(() => {
    monaco.editor.create(container, {
      value: 'SELECT * FROM narf',
      language: 'sql',
    });
  });
</script>

<div class="h-full text-left w-full monaco-container" bind:this={container} />
