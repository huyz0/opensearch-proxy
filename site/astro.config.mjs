import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import remarkMermaid from "./remark-mermaid.mjs";

const repo = "https://github.com/huyz0/opensearch-proxy";

// Loads mermaid from a CDN and renders every <pre class="mermaid"> on each page,
// including after Starlight's client-side navigation. Per-diagram %%{init}%%
// blocks set their own colors, so contrast holds in both light and dark themes.
const mermaidBoot = `
import mermaid from "https://esm.sh/mermaid@11";
function runMermaid() {
  const dark = document.documentElement.dataset.theme === "dark";
  mermaid.initialize({ startOnLoad: false, theme: dark ? "dark" : "default", securityLevel: "loose", fontFamily: "inherit" });
  mermaid.run({ querySelector: "pre.mermaid:not([data-rendered])" });
  document.querySelectorAll("pre.mermaid").forEach((el) => el.setAttribute("data-rendered", "true"));
}
document.addEventListener("astro:page-load", runMermaid);
if (document.readyState !== "loading") runMermaid();
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
      head: [{ tag: "script", attrs: { type: "module" }, content: mermaidBoot }],
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
