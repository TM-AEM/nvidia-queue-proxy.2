#!/usr/bin/env node
// Canonical-deployment guard (PHASE 1.3 / 7.2).
//
// Fails if the repository contains more than one production Wrangler
// configuration or more than one Worker entry point, or if the single
// canonical config's `main` does not resolve to an existing file. This makes
// an accidental deployment of a stale or duplicate Worker impossible in CI.

import { readFileSync, existsSync } from "node:fs";
import { readdirSync, statSync } from "node:fs";
import { dirname, join, relative, sep } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..");

const SKIP_DIRS = new Set(["node_modules", ".git", "dist", "build", ".wrangler", ".openhands"]);
const WRANGLER_NAMES = ["wrangler.jsonc", "wrangler.json", "wrangler.toml"];
const WORKER_ENTRY_NAMES = new Set(["index.js", "index.mjs", "index.ts", "worker.js"]);

function walk(dir, files = []) {
  for (const entry of readdirSync(dir)) {
    if (SKIP_DIRS.has(entry)) continue;
    const full = join(dir, entry);
    const stat = statSync(full);
    if (stat.isDirectory()) {
      walk(full, files);
    } else {
      files.push(full);
    }
  }
  return files;
}

const files = walk(repoRoot);
const rel = (f) => relative(repoRoot, f);

const wranglerConfigs = files.filter((f) => WRANGLER_NAMES.includes(f.split(sep).pop()));
const workerEntries = files.filter(
  (f) => rel(f).startsWith(`src${sep}`) && WORKER_ENTRY_NAMES.has(f.split(sep).pop())
);

const errors = [];

if (wranglerConfigs.length !== 1) {
  errors.push(
    `expected exactly one Wrangler config, found ${wranglerConfigs.length}: ${wranglerConfigs
      .map(rel)
      .join(", ") || "(none)"}`
  );
}

if (workerEntries.length !== 1) {
  errors.push(
    `expected exactly one Worker entry point under src/, found ${workerEntries.length}: ${workerEntries
      .map(rel)
      .join(", ") || "(none)"}`
  );
}

if (wranglerConfigs.length === 1) {
  const configPath = wranglerConfigs[0];
  let main;

  try {
    const raw = readFileSync(configPath, "utf8");
    const json = configPath.endsWith(".toml") ? null : JSON.parse(raw);
    main = json?.main;
  } catch (error) {
    errors.push(`could not parse ${rel(configPath)}: ${error.message}`);
  }

  if (!main) {
    errors.push(`${rel(configPath)} does not declare a "main" entry point`);
  } else if (!existsSync(join(repoRoot, main))) {
    errors.push(`${rel(configPath)} main "${main}" does not resolve to an existing file`);
  } else if (workerEntries.length === 1 && rel(join(repoRoot, main)) !== rel(workerEntries[0])) {
    errors.push(
      `${rel(configPath)} main "${main}" is not the canonical Worker entry ${rel(workerEntries[0])}`
    );
  }
}

if (errors.length > 0) {
  console.error("Canonical deployment guard FAILED:");
  for (const error of errors) {
    console.error(`  - ${error}`);
  }
  process.exit(1);
}

console.log(
  `Canonical deployment guard passed: 1 config (${rel(wranglerConfigs[0])}), ` +
    `1 Worker entry (${rel(workerEntries[0])}).`
);
