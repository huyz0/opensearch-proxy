// Copies the canonical guide markdown from ../docs/guide into the Starlight
// content collection, adding the frontmatter Starlight needs and rewriting links.
//
// One source of truth stays docs/guide/*.md (browsable on GitHub). This script
// generates site/src/content/docs/*.md at build time:
//   - README.md            -> index.md          (site root)
//   - NN-name.md           -> NN-name.md         (/<base>/NN-name/)
//   - the first "# Title"  -> frontmatter title  (Starlight renders the H1)
//   - intra-guide .md links -> on-site routes
//   - ../ and ../../ links  -> GitHub blob URLs (those files are not on the site)

import { readFileSync, writeFileSync, readdirSync, mkdirSync, rmSync } from "node:fs";
import path from "node:path";

const SRC = path.resolve("../docs/guide");
const OUT = path.resolve("src/content/docs");
const BASE = "/opensearch-proxy";
const REPO_BLOB = "https://github.com/huyz0/opensearch-proxy/blob/main";

function yamlEscape(s) {
  return s.replace(/"/g, '\\"');
}

function rewriteLinks(body) {
  return body.replace(/\]\(([^)]+)\)/g, (whole, target) => {
    // Leave external URLs, mail, and pure in-page anchors untouched.
    if (/^(https?:|mailto:|#)/.test(target)) return whole;

    const [pathPart, frag] = target.split("#");
    const anchor = frag ? `#${frag}` : "";

    // Links that climb out of docs/guide point at files not on the site.
    if (pathPart.startsWith("../")) {
      const repoPath = path.posix.normalize(path.posix.join("docs/guide", pathPart));
      return `](${REPO_BLOB}/${repoPath}${anchor})`;
    }

    // Intra-guide markdown links become on-site routes.
    if (pathPart.endsWith(".md")) {
      const name = pathPart.replace(/^\.\//, "").replace(/\.md$/, "");
      const route = name === "README" ? `${BASE}/` : `${BASE}/${name}/`;
      return `](${route}${anchor})`;
    }
    return whole;
  });
}

function convert(file) {
  const raw = readFileSync(path.join(SRC, file), "utf8");
  const lines = raw.split("\n");
  const h1 = lines.findIndex((l) => l.startsWith("# "));
  const title = h1 >= 0 ? lines[h1].replace(/^#\s+/, "").trim() : file;
  if (h1 >= 0) lines.splice(h1, 1); // Starlight renders the title as the H1.

  const body = rewriteLinks(lines.join("\n").replace(/^\n+/, ""));
  const front = [`---`, `title: "${yamlEscape(title)}"`, `---`, ``].join("\n");

  const outName = file === "README.md" ? "index.md" : file;
  writeFileSync(path.join(OUT, outName), front + body);
  return outName;
}

rmSync(OUT, { recursive: true, force: true });
mkdirSync(OUT, { recursive: true });
const written = readdirSync(SRC)
  .filter((f) => f.endsWith(".md"))
  .map(convert);
console.log(`synced ${written.length} pages -> ${OUT}`);
