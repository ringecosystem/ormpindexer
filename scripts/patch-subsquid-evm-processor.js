const fs = require("fs");
const path = require("path");

const root = path.resolve(__dirname, "..");

function patchFile(file, patches) {
  if (!fs.existsSync(file)) {
    console.warn(`[patch-subsquid-evm-processor] skipped missing ${file}`);
    return;
  }

  let source = fs.readFileSync(file, "utf8");
  let changed = false;

  for (const [from, to] of patches) {
    if (source.includes(to)) {
      continue;
    }
    if (!source.includes(from)) {
      throw new Error(`Patch target not found in ${file}: ${from}`);
    }
    source = source.replace(from, to);
    changed = true;
  }

  if (changed) {
    fs.writeFileSync(file, source);
    console.log(`[patch-subsquid-evm-processor] patched ${file}`);
  }
}

patchFile(path.join(root, "node_modules/@subsquid/evm-processor/src/ds-rpc/rpc.ts"), [
  [
    "    if (/response is too big/i.test(err.message)) return true\n",
    "    if (/response is too big/i.test(err.message)) return true\n    if (/query returns too many logs/i.test(err.message)) return true\n",
  ],
]);

patchFile(path.join(root, "node_modules/@subsquid/evm-processor/lib/ds-rpc/rpc.js"), [
  [
    "    if (/response is too big/i.test(err.message))\n        return true;\n",
    "    if (/response is too big/i.test(err.message))\n        return true;\n    if (/query returns too many logs/i.test(err.message))\n        return true;\n",
  ],
]);

patchFile(path.join(root, "node_modules/@subsquid/evm-processor/src/ds-rpc/client.ts"), [
  [
    "                    for (let range of splitRange(10, split.range)) {",
    "                    for (let range of splitRange(getHotBlockRangeSize(), split.range)) {",
  ],
  [
    "\n\nconst NewHeadMessage = object({",
    "\n\nfunction getHotBlockRangeSize(): number {\n    let value = Number(process.env.SQUID_EVM_HOT_BLOCK_RANGE_SIZE ?? 1)\n    if (!Number.isSafeInteger(value) || value < 1) return 1\n    return value\n}\n\nconst NewHeadMessage = object({",
  ],
]);

patchFile(path.join(root, "node_modules/@subsquid/evm-processor/lib/ds-rpc/client.js"), [
  [
    "                    for (let range of (0, util_internal_range_1.splitRange)(10, split.range)) {",
    "                    for (let range of (0, util_internal_range_1.splitRange)(getHotBlockRangeSize(), split.range)) {",
  ],
  [
    "\nconst NewHeadMessage =",
    "\nfunction getHotBlockRangeSize() {\n    let value = Number(process.env.SQUID_EVM_HOT_BLOCK_RANGE_SIZE ?? 1);\n    if (!Number.isSafeInteger(value) || value < 1)\n        return 1;\n    return value;\n}\nconst NewHeadMessage =",
  ],
]);
