// Turns fenced ```mermaid code blocks into <pre class="mermaid"> so the diagram
// is rendered client-side by mermaid.js (loaded in astro.config head) instead of
// being syntax-highlighted as a code block by Starlight's Expressive Code.
import { visit } from "unist-util-visit";

export default function remarkMermaid() {
  return (tree) => {
    visit(tree, "code", (node, index, parent) => {
      if (!parent || node.lang !== "mermaid") return;
      // Escape so the browser stores the original source as textContent, which is
      // what mermaid reads. `<br/>` becomes &lt;br/&gt; -> textContent "<br/>".
      const escaped = node.value
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");
      parent.children[index] = {
        type: "html",
        value: `<pre class="mermaid">${escaped}</pre>`,
      };
    });
  };
}
