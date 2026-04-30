/**
 * Removes a uniform background (RGBA PNG output).
 * 1) Edge flood-fill — exterior bg connected to image borders.
 * 2) Global bg match — enclosed holes (e.g. gaps between letters) match bg but are not edge-connected.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import sharp from "sharp";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const INPUT = path.join(ROOT, "UpdatedLogo.png");
const OUT_LOGO = path.join(ROOT, "public", "logo.png");
const OUT_ICON = path.join(ROOT, "public", "app-icon.png");
const OUT_SOURCE = path.join(ROOT, "UpdatedLogo.png");

/** Max per-channel distance from reference background (edge-connected pixels become transparent). */
const FUZZ = 38;

async function rgbaFromSharp(inputPath) {
  const { data, info } = await sharp(inputPath).ensureAlpha().raw().toBuffer({ resolveWithObject: true });
  return { buf: Buffer.from(data), width: info.width, height: info.height, channels: info.channels };
}

function index(w, x, y) {
  return (y * w + x) * 4;
}

/**
 * Flood-fill from image edges to remove background connected to borders.
 */
function floodTransparentEdge(buf, width, height, fuzz) {
  const bgIdx = index(width, 0, 0);
  const br = buf[bgIdx];
  const bg = buf[bgIdx + 1];
  const bb = buf[bgIdx + 2];

  function matches(pi) {
    return (
      Math.abs(buf[pi] - br) <= fuzz &&
      Math.abs(buf[pi + 1] - bg) <= fuzz &&
      Math.abs(buf[pi + 2] - bb) <= fuzz
    );
  }

  const seen = new Uint8Array(width * height);
  const stack = [];

  function tryPush(x, y) {
    if (x < 0 || y < 0 || x >= width || y >= height) return;
    const i = y * width + x;
    if (seen[i]) return;
    const pi = i * 4;
    if (!matches(pi)) return;
    seen[i] = 1;
    buf[pi + 3] = 0;
    stack.push(x, y);
  }

  for (let x = 0; x < width; x++) {
    tryPush(x, 0);
    tryPush(x, height - 1);
  }
  for (let y = 0; y < height; y++) {
    tryPush(0, y);
    tryPush(width - 1, y);
  }

  while (stack.length) {
    const y = stack.pop();
    const x = stack.pop();
    tryPush(x + 1, y);
    tryPush(x - 1, y);
    tryPush(x, y + 1);
    tryPush(x, y - 1);
  }
}

/**
 * Pixels inside letter-shaped “holes” match the background RGB but were never reached from the border.
 * Second pass: clear alpha anywhere color still matches the sampled background (same fuzz as edge pass).
 */
function punchInteriorBackgroundMatches(buf, width, height, br, bg, bb, fuzz) {
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const pi = index(width, x, y);
      if (
        Math.abs(buf[pi] - br) <= fuzz &&
        Math.abs(buf[pi + 1] - bg) <= fuzz &&
        Math.abs(buf[pi + 2] - bb) <= fuzz
      ) {
        buf[pi + 3] = 0;
      }
    }
  }
}

async function main() {
  if (!fs.existsSync(INPUT)) {
    console.error("Missing", INPUT);
    process.exit(1);
  }

  const { buf, width, height } = await rgbaFromSharp(INPUT);
  const br = buf[0];
  const bg = buf[1];
  const bb = buf[2];
  floodTransparentEdge(buf, width, height, FUZZ);
  punchInteriorBackgroundMatches(buf, width, height, br, bg, bb, FUZZ);

  const png = await sharp(buf, { raw: { width, height, channels: 4 } }).png().toBuffer();

  fs.mkdirSync(path.dirname(OUT_LOGO), { recursive: true });
  fs.writeFileSync(OUT_LOGO, png);
  fs.writeFileSync(OUT_ICON, png);
  fs.writeFileSync(OUT_SOURCE, png);

  const verify = await sharp(png).metadata();
  console.log(
    "[make-logo-transparent] Wrote transparent PNG:",
    OUT_LOGO,
    OUT_ICON,
    OUT_SOURCE,
    `(${verify.width}x${verify.height}, hasAlpha: ${verify.hasAlpha})`,
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
