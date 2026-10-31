// Copy the browser bundle into the Python package, so FastAPI apps can serve it
// without any JavaScript tooling (see xferkit.integrations.fastapi.mount_browser_client).
import { copyFileSync, mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const source = resolve(here, "../dist/xferkit.mjs");
const target = resolve(here, "../../../src/xferkit/static/xferkit.mjs");
mkdirSync(dirname(target), { recursive: true });
copyFileSync(source, target);
console.log(`copied ${source} -> ${target}`);
