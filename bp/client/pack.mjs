// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Assemble the @browserproject/selkies-client package from the viewer cores
// in addons/selkies-web-core: the files of files.txt, their .d.ts types and
// package.json at the given version.
//
//   node bp/client/pack.mjs <version> <out dir>
//
// Needs `npm ci` in bp/client/tools first (TypeScript for the types). The
// types are emitted without a type check: the upstream-derived cores are
// plain JavaScript, never type-checked.

import { execFileSync } from "node:child_process";
import { cpSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, normalize, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "../..");
const source = join(root, "addons/selkies-web-core");
const [version, outArg] = process.argv.slice(2);
if (!version || !outArg) {
  console.error("usage: node bp/client/pack.mjs <version> <out dir>");
  process.exit(2);
}
const out = resolve(outArg);

const files = readFileSync(join(here, "files.txt"), "utf8").split("\n").map((l) => l.trim()).filter(Boolean);
const listed = new Set(files);

// Every relative import of a packaged file must be packaged too. Comment
// lines are skipped: the hook markers quote import statements.
const specifier = /(?:\bfrom\s*|\bimport\s*\(\s*|^\s*import\s*)["'](\.{1,2}\/[^"']+)["']/g;
const comment = /^\s*(?:\*|\/\/|\/\*)/;
const missing = [];
for (const file of files) {
  const lines = readFileSync(join(source, file), "utf8").split("\n").filter((l) => !comment.test(l));
  for (const match of lines.join("\n").matchAll(specifier)) {
    const target = normalize(join(dirname(file), match[1]));
    if (!listed.has(target)) missing.push(`${file} imports ${target}`);
  }
}
if (missing.length) {
  console.error(`files.txt misses imported files:\n${missing.join("\n")}`);
  process.exit(1);
}

rmSync(out, { recursive: true, force: true });
mkdirSync(out, { recursive: true });
for (const file of files) cpSync(join(source, file), join(out, file));
cpSync(join(root, "LICENSE"), join(out, "LICENSE"));
cpSync(join(here, "README.md"), join(out, "README.md"));

const manifest = JSON.parse(readFileSync(join(here, "package.json"), "utf8"));
manifest.version = version;
manifest.files = [...files, ...files.map((f) => f.replace(/\.js$/, ".d.ts")), "LICENSE", "README.md"];
writeFileSync(join(out, "package.json"), `${JSON.stringify(manifest, null, 2)}\n`);

execFileSync(join(here, "tools/node_modules/.bin/tsc"), [
  "--allowJs", "--declaration", "--emitDeclarationOnly", "--noCheck", "--skipLibCheck",
  "--target", "es2022", "--module", "esnext", "--moduleResolution", "bundler",
  "--lib", "es2023,dom,dom.iterable,webworker.importscripts",
  "--rootDir", out, "--outDir", out,
  ...files.map((f) => join(out, f)),
], { stdio: "inherit" });

console.log(`packed ${manifest.name}@${version} (${files.length} modules) into ${relative(process.cwd(), out) || "."}`);
