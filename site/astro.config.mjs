import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import remarkMermaid from "./remark-mermaid.mjs";

const repo = "https://github.com/huyz0/opensearch-proxy";

// Loads mermaid from a CDN and renders every <pre class="mermaid"> on each page,
// including after Starlight's client-side navigation. Per-diagram %%{init}%%
// blocks set their own colors, so contrast holds in both light and dark themes.
//
// Two readability affordances on top of the raw render:
//   1. `useMaxWidth: false` — mermaid's default shrinks any diagram wider than the
//      content column to fit it, scaling the font down with it (the "tiny diagram"
//      problem). Disabling it keeps the natural font size; the `.mermaid` wrapper
//      scrolls horizontally instead (see mermaidCss).
//   2. Click-to-zoom lightbox — a rendered diagram opens full-viewport on click and
//      dismisses on click/Escape, so the big flowcharts are legible without a
//      separate image pipeline. Dependency-free; re-armed on every page-load.
const mermaidBoot = `
import mermaid from "https://esm.sh/mermaid@11";
function armZoom() {
  document.querySelectorAll("pre.mermaid:not([data-zoom])").forEach((el) => {
    el.setAttribute("data-zoom", "true");
    el.style.cursor = "zoom-in";
    el.addEventListener("click", () => {
      const svg = el.querySelector("svg");
      if (!svg) return;
      const overlay = document.createElement("div");
      overlay.className = "mermaid-lightbox";
      overlay.innerHTML = el.innerHTML;
      const close = () => overlay.remove();
      overlay.addEventListener("click", close);
      const onKey = (e) => { if (e.key === "Escape") { close(); document.removeEventListener("keydown", onKey); } };
      document.addEventListener("keydown", onKey);
      document.body.appendChild(overlay);
    });
  });
}
function runMermaid() {
  const dark = document.documentElement.dataset.theme === "dark";
  mermaid.initialize({ startOnLoad: false, theme: dark ? "dark" : "default", securityLevel: "loose", fontFamily: "inherit", flowchart: { useMaxWidth: false }, sequence: { useMaxWidth: false } });
  mermaid.run({ querySelector: "pre.mermaid:not([data-rendered])" }).then(armZoom);
  document.querySelectorAll("pre.mermaid").forEach((el) => el.setAttribute("data-rendered", "true"));
}
document.addEventListener("astro:page-load", runMermaid);
if (document.readyState !== "loading") runMermaid();
`;

// Keeps a too-wide diagram readable (horizontal scroll, natural font) rather than
// shrunk, and styles the click-to-zoom lightbox. The overlay sizes the SVG to the
// viewport so the largest flowcharts are fully legible.
const mermaidCss = `
pre.mermaid { overflow-x: auto; text-align: center; }
pre.mermaid svg { max-width: none; height: auto; }
.mermaid-lightbox {
  position: fixed; inset: 0; z-index: 1000; cursor: zoom-out;
  display: flex; align-items: center; justify-content: center;
  padding: 2rem; background: rgba(0, 0, 0, 0.75);
}
.mermaid-lightbox svg { max-width: 96vw; max-height: 92vh; width: auto; height: auto; }
`;

export default defineConfig({
  site: "https://huyz0.github.io",
  base: "/opensearch-proxy",
  markdown: { remarkPlugins: [remarkMermaid] },
  integrations: [
    starlight({
      title: "osproxy",
      description:
        "A high-performance OpenSearch routing proxy you consume as a Rust library.",
      social: [{ icon: "github", label: "GitHub", href: repo }],
      head: [
        { tag: "script", attrs: { type: "module" }, content: mermaidBoot },
        { tag: "style", content: mermaidCss },
      ],
      sidebar: [
        { label: "Start here", items: [{ label: "User Guide", link: "/" }] },
        {
          label: "Guide",
          items: [
            { label: "1. Overview & Intent", link: "/01-overview/" },
            { label: "2. Requirements & NFRs", link: "/02-requirements-and-nfrs/" },
            { label: "3. Architecture", link: "/03-architecture/" },
            { label: "4. Components", link: "/04-components/" },
            { label: "5. The SPI", link: "/05-spi-guide/" },
            { label: "6. Wiring It Together", link: "/06-wiring-example/" },
            { label: "7. Configuration", link: "/07-configuration/" },
            { label: "8. Observability & Control Plane", link: "/08-observability/" },
            { label: "9. Async Fan-out Clients", link: "/09-async-clients/" },
            { label: "10. Choosing a Mode", link: "/10-choosing-a-mode/" },
            { label: "11. Performance", link: "/11-performance/" },
          ],
        },
      ],
    }),
  ],
});
