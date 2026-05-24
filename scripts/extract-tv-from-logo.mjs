/**
 * Crop the TV mark from the left side of public/logo.png (wordmark on the right).
 * Tune TV_CROP_WIDTH if the crop is too tight/loose.
 * Run: node scripts/extract-tv-from-logo.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import sharp from "sharp";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const logoPath = path.join(root, "public", "logo.png");
const outPath = path.join(root, "public", "brand-tv-from-logo.png");

/** Pixels from the left edge of logo.png to keep (TV + glow sit in this band). */
const TV_CROP_WIDTH = 620;

/** Recenter artwork so luminance centroid ≈ geometric center (fixes “icon sits left”). */
function luminanceCentroid(buf, w, h, ch) {
  let sum = 0;
  let mx = 0;
  let my = 0;
  for (let y = 0; y < h; y++) {
    for (let x = 0; x < w; x++) {
      const i = (y * w + x) * ch;
      const lum =
        (0.2126 * buf[i] + 0.7152 * buf[i + 1] + 0.0722 * buf[i + 2]) * (buf[i + 3] / 255);
      if (lum > 8) {
        sum += lum;
        mx += x * lum;
        my += y * lum;
      }
    }
  }
  if (sum < 1e-6) return { cx: w / 2, cy: h / 2 };
  return { cx: mx / sum, cy: my / sum };
}

async function opticalPadTransparent(buf) {
  const { data, info } = await sharp(buf).ensureAlpha().raw().toBuffer({ resolveWithObject: true });
  const w = info.width;
  const h = info.height;
  const ch = info.channels;
  const { cx, cy } = luminanceCentroid(data, w, h, ch);
  const padLeft = cx < w / 2 ? Math.max(0, Math.round(w - 2 * cx)) : 0;
  const padRight = cx > w / 2 ? Math.max(0, Math.round(2 * cx - w)) : 0;
  const padTop = cy < h / 2 ? Math.max(0, Math.round(h - 2 * cy)) : 0;
  const padBottom = cy > h / 2 ? Math.max(0, Math.round(2 * cy - h)) : 0;
  if (padLeft + padRight + padTop + padBottom === 0) return buf;
  return sharp(buf)
    .extend({
      left: padLeft,
      right: padRight,
      top: padTop,
      bottom: padBottom,
      background: { r: 0, g: 0, b: 0, alpha: 0 },
    })
    .png({ compressionLevel: 9 })
    .toBuffer();
}

async function main() {
  const meta = await sharp(logoPath).metadata();
  if (!meta.width || !meta.height) throw new Error("Could not read logo.png dimensions");
  const w = Math.min(TV_CROP_WIDTH, meta.width);
  const buf = await sharp(logoPath)
    .extract({ left: 0, top: 0, width: w, height: meta.height })
    .png({ compressionLevel: 9 })
    .toBuffer();
  const trimmed = await sharp(buf).trim({ threshold: 12 }).png({ compressionLevel: 9 }).toBuffer();
  const centered = await opticalPadTransparent(trimmed);
  fs.writeFileSync(outPath, centered);
  const outMeta = await sharp(centered).metadata();
  console.log("Wrote", path.relative(root, outPath), `${outMeta.width}x${outMeta.height}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
