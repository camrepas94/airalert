/**
 * Composite store-ready PNGs: nebula background (SVG) + TV raster from logo.png.
 * Requires: public/brand-icon-bg.svg, public/brand-tv-from-logo.png (run extract-tv-from-logo.mjs first).
 * Run: node scripts/render-app-icons.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import sharp from "sharp";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const outDir = path.join(root, "public");
const bgSvgPath = path.join(outDir, "brand-icon-bg.svg");
const tvPngPath = path.join(outDir, "brand-tv-from-logo.png");

const bgSvg = fs.readFileSync(bgSvgPath);
const sizes = [
  ["app-icon-1024.png", 1024],
  ["app-icon-512.png", 512],
  ["app-icon-192.png", 192],
  ["app-icon-180.png", 180],
];

/** Match `brand-icon.svg`: TV sits in a centered square slot 840/1024 of the canvas. */
const TV_SLOT_FRAC = 840 / 1024;

async function renderSquare(size) {
  const bg = await sharp(bgSvg, { density: 400 })
    .resize(size, size, { fit: "fill" })
    .ensureAlpha()
    .png()
    .toBuffer();

  const tvMax = Math.round(size * TV_SLOT_FRAC);
  const tvBuf = await sharp(tvPngPath)
    .resize({
      width: tvMax,
      height: tvMax,
      fit: "inside",
      kernel: sharp.kernel.lanczos3,
    })
    .ensureAlpha()
    .toBuffer();

  const { width: tw = 0, height: th = 0 } = await sharp(tvBuf).metadata();
  const left = Math.max(0, Math.round((size - tw) / 2));
  const top = Math.max(0, Math.round((size - th) / 2));

  return sharp(bg).composite([{ input: tvBuf, left, top }]).png({ compressionLevel: 9 }).toBuffer();
}

for (const [name, size] of sizes) {
  const png = await renderSquare(size);
  fs.writeFileSync(path.join(outDir, name), png);
}

fs.writeFileSync(path.join(outDir, "app-icon.png"), await renderSquare(512));

console.log("Wrote:", sizes.map(([n]) => n).join(", "), ", app-icon.png");
