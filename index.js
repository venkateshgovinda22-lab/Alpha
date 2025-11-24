/**
 * index.js
 *
 * Industrial-grade ES Module scraper (Puppeteer + Firebase client SDK + Telegram).
 * - Validates required envs early and fails fast.
 * - Uses client-side Firebase SDK (modular) via FIREBASE_CONFIG or FIREBASE_CONFIG_PATH.
 * - Robust selector retries, exponential backoff, and Firestore batch commits with retries.
 * - Date extraction removes time components and preserves human-readable month formats.
 *
 * Required envs (set via CI / local):
 * - SCRAPE_URL                     (required) target page to scrape
 * - FIREBASE_CONFIG OR FIREBASE_CONFIG_PATH  (required) client Firebase config JSON or path
 * Optional:
 * - PUPPETEER_HEADLESS ("false" to run with UI)
 * - TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
 * - LIST_CONTAINER_SELECTOR, ITEM_ROW_SELECTOR
 *
 * Notes:
 * - This file is ESM (package.json "type": "module").
 * - The workflow should run Node 18+ (recommended) so global fetch is available.
 */

import puppeteer from 'puppeteer';
import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

import { initializeApp } from 'firebase/app';
import {
  getFirestore,
  writeBatch,
  doc as firestoreDoc,
  serverTimestamp,
} from 'firebase/firestore';

const __filename = fileURLToPath(import.meta.url);

const DEFAULT_SELECTOR_TIMEOUT = 8_000; // ms
const RETRY_ATTEMPTS = 3;
const RETRY_BASE_DELAY = 500; // ms
const FIRESTORE_BATCH_MAX = 400; // under 500 limit

// Structured loggers
const info = (...args) => console.info(new Date().toISOString(), '[INFO]', ...args);
const warn = (...args) => console.warn(new Date().toISOString(), '[WARN]', ...args);
const error = (...args) => console.error(new Date().toISOString(), '[ERROR]', ...args);

/* ----------------------------- Helpers ----------------------------- */

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function retry(fn, { attempts = RETRY_ATTEMPTS, baseDelay = RETRY_BASE_DELAY, onRetry } = {}) {
  let i = 0;
  while (i < attempts) {
    try {
      return await fn();
    } catch (err) {
      i += 1;
      if (i >= attempts) throw err;
      const delay = baseDelay * Math.pow(2, i - 1);
      if (typeof onRetry === 'function') onRetry(i, err, delay);
      await sleep(delay);
    }
  }
}

async function waitForSelectorWithRetries(page, selector, opts = {}) {
  const timeout = opts.timeout ?? DEFAULT_SELECTOR_TIMEOUT;
  const attempts = opts.attempts ?? RETRY_ATTEMPTS;
  return retry(
    async () => {
      const el = await page.waitForSelector(selector, { timeout });
      if (!el) throw new Error(`Selector "${selector}" not found`);
      return el;
    },
    {
      attempts,
      baseDelay: opts.baseDelay ?? RETRY_BASE_DELAY,
      onRetry: (attempt, err, delay) =>
        warn(`Retry ${attempt}/${attempts} for selector "${selector}" after: ${err.message}. Backoff ${delay}ms`),
    }
  );
}

/* ----------------------------- Date / Time Utilities ----------------------------- */

function formatDateISO(date) {
  if (!(date instanceof Date)) return null;
  const y = date.getFullYear();
  const m = (`0${date.getMonth() + 1}`).slice(-2);
  const d = (`0${date.getDate()}`).slice(-2);
  return `${y}-${m}-${d}`;
}

function parseFlexibleDate(text) {
  if (!text || typeof text !== 'string') return null;
  const parsed = Date.parse(text);
  if (!Number.isNaN(parsed)) return new Date(parsed);
  const now = new Date();
  const withYear = `${text} ${now.getFullYear()}`;
  const parsed2 = Date.parse(withYear);
  if (!Number.isNaN(parsed2)) return new Date(parsed2);
  return null;
}

/**
 * Extract a date-only string from raw text (strip times & zones).
 * Returns either a preserved human-readable month format (e.g., "Dec 20, 2025"),
 * or an ISO date (YYYY-MM-DD), or null if unparseable.
 */
function extractDateOnly(rawText, now = new Date()) {
  if (!rawText || typeof rawText !== 'string') return null;
  let s = rawText.trim().replace(/\s+/g, ' ');
  const lowered = s.toLowerCase();

  // Relative terms
  if (/\btoday\b/.test(lowered)) {
    return formatDateISO(new Date(now.getFullYear(), now.getMonth(), now.getDate()));
  }
  if (/\byesterday\b/.test(lowered)) {
    const d = new Date(now);
    d.setDate(d.getDate() - 1);
    return formatDateISO(new Date(d.getFullYear(), d.getMonth(), d.getDate()));
  }

  // Remove 'at', timezone tokens and time components
  s = s.replace(/\bat\s+/i, ' ');
  s = s.replace(/\b(?:[A-Z]{2,5}|GMT[+\-]?\d{1,2}|UTC[+\-]?\d{1,2})\b/gi, '');
  s = s.replace(/\b\d{1,2}:\d{2}(?::\d{2})?\s?(?:am|pm|AM|PM)?\b/g, '');
  s = s.replace(/\b\d{1,2}\s?(?:am|pm)\b/gi, '');
  s = s.replace(/\b\d{3,4}hrs?\b/gi, '').replace(/\b\d{2}:\d{2}:\d{2}\b/g, '');
  s = s.replace(/\b(posted|posted on|posted:)\b/gi, '');
  s = s.replace(/[|–—•]/g, ' ');
  s = s.replace(/\s+/g, ' ').trim();

  const monthPattern = /\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b/i;
  if (monthPattern.test(s)) {
    const human = s.replace(/,\s*$/, '').trim();
    if (!/:\d{2}/.test(human)) return human; // preserve human-readable month/day format
  }

  const parsedDate = parseFlexibleDate(s);
  if (parsedDate) return formatDateISO(parsedDate);

  const mmdd = s.match(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b/);
  if (mmdd) {
    const month = parseInt(mmdd[1], 10);
    const day = parseInt(mmdd[2], 10);
    const year = mmdd[3] ? parseInt(mmdd[3], 10) : now.getFullYear();
    if (month >= 1 && month <= 12 && day >= 1 && day <= 31) {
      return formatDateISO(new Date(year, month - 1, day));
    }
  }

  return null;
}

/* ----------------------------- Firebase Client Init ----------------------------- */

/**
 * Initialize Firebase client SDK (modular). Returns { app, db }.
 * Requires FIREBASE_CONFIG (JSON string) or FIREBASE_CONFIG_PATH (file path).
 */
function initFirebaseClient() {
  let firebaseConfig = null;
  if (process.env.FIREBASE_CONFIG) {
    try {
      firebaseConfig = JSON.parse(process.env.FIREBASE_CONFIG);
    } catch (err) {
      throw new Error('FIREBASE_CONFIG is not valid JSON.');
    }
  } else if (process.env.FIREBASE_CONFIG_PATH) {
    const providedPath = process.env.FIREBASE_CONFIG_PATH;
    const resolved = path.isAbsolute(providedPath) ? providedPath : path.resolve(process.cwd(), providedPath);
    try {
      const raw = fs.readFileSync(resolved, 'utf8');
      firebaseConfig = JSON.parse(raw);
    } catch (err) {
      throw new Error(`Failed loading FIREBASE_CONFIG from "${resolved}": ${err.message}`);
    }
  } else {
    throw new Error('Either FIREBASE_CONFIG (JSON string) or FIREBASE_CONFIG_PATH must be provided.');
  }

  const app = initializeApp(firebaseConfig);
  const db = getFirestore(app);
  info('Initialized Firebase client SDK.');
  return { app, db };
}

/* ----------------------------- Telegram ----------------------------- */

async function sendTelegramMessage(text) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) {
    warn('Telegram not configured (TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing). Skipping notification.');
    return;
  }
  if (typeof globalThis.fetch !== 'function') {
    warn('global fetch not available; skipping Telegram notification to avoid adding dependencies.');
    return;
  }
  const url = `https://api.telegram.org/bot${token}/sendMessage`;
  try {
    const res = await fetch(url, {
      method: 'POST',
      body: JSON.stringify({ chat_id: chatId, text }),
      headers: { 'Content-Type': 'application/json' },
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) {
      warn('Failed to send Telegram message:', res.status, body);
    } else {
      info('Telegram notification sent.');
    }
  } catch (err) {
    warn('Error sending Telegram message:', err && (err.message || err));
  }
}

/* ----------------------------- Main Scraper ----------------------------- */

async function runScraper() {
  const startTime = Date.now();
  info('Scraper starting');

  // Fail fast: validate required envs before heavy initialization
  const scrapeUrl = process.env.SCRAPE_URL;
  if (!scrapeUrl) {
    const msg = 'SCRAPE_URL environment variable is required. Example: SCRAPE_URL="https://example.com/jobs"';
    error(msg);
    throw new Error('SCRAPE_URL environment variable is required.');
  }

  // Initialize Firebase only after validation (saves time & avoids noisy logs)
  const { db } = initFirebaseClient();

  const headless = process.env.PUPPETEER_HEADLESS !== 'false';

  let browser;
  try {
    browser = await puppeteer.launch({
      headless,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--single-process',
        '--no-zygote',
      ],
    });

    const page = await browser.newPage();
    page.setDefaultNavigationTimeout(60_000);

    info('Navigating to', scrapeUrl);
    await retry(
      () =>
        page.goto(scrapeUrl, {
          waitUntil: ['domcontentloaded', 'networkidle2'],
          timeout: 60_000,
        }),
      {
        attempts: 3,
        baseDelay: 1000,
        onRetry: (a, err) => warn(`page.goto retry ${a} due to: ${err.message}`),
      }
    );

    const listContainerSelector = process.env.LIST_CONTAINER_SELECTOR || '.job-listing, .results, #results';
    try {
      await waitForSelectorWithRetries(page, listContainerSelector, { attempts: 2 });
    } catch (err) {
      warn(`List container not found (${listContainerSelector}); continuing with best-effort extraction.`);
    }

    // Extract items using a single page evaluation for speed
    const itemsSelector = process.env.ITEM_ROW_SELECTOR || '.job-row, .listing, article, tr';
    const jobs = await page.$$eval(itemsSelector, (nodes) => {
      const results = [];
      for (const node of nodes) {
        try {
          const titleEl = node.querySelector('.title, .job-title, h2, a') || null;
          const companyEl = node.querySelector('.company, .employer') || null;
          const locationEl = node.querySelector('.location') || null;
          const dateEl = node.querySelector('.date, .posted, .post-date, .green-row') || null;
          const linkEl = node.querySelector('a[href]') || null;

          const title = titleEl ? titleEl.innerText.trim() : null;
          const company = companyEl ? companyEl.innerText.trim() : null;
          const location = locationEl ? locationEl.innerText.trim() : null;
          const rawDate = dateEl ? dateEl.innerText.trim() : null;
          const url = linkEl ? linkEl.href : null;
          const idAttr = node.getAttribute('data-id') || node.id || url || (title ? title.slice(0, 80) : null);

          results.push({ title, company, location, rawDate, url, idAttr });
        } catch (e) {
          // ignore bad node
        }
      }
      return results;
    });

    info(`Scraped ${jobs.length} candidate entries.`);

    // Normalize, dedupe and prepare records
    const normalized = [];
    const seen = new Set();
    for (const item of jobs) {
      if (!item.title && !item.company) continue;
      const idSource = item.url || item.idAttr || `${item.title || ''}::${item.company || ''}`;
      const id = crypto.createHash('sha256').update(idSource).digest('hex');
      if (seen.has(id)) continue;
      seen.add(id);

      const dateOnly = extractDateOnly(item.rawDate);
      normalized.push({
        id,
        title: item.title || null,
        company: item.company || null,
        location: item.location || null,
        rawDate: item.rawDate || null,
        date: dateOnly, // human-readable (e.g., "Dec 20, 2025") or ISO "YYYY-MM-DD" or null
        url: item.url || null,
      });
    }

    info(`Normalized ${normalized.length} unique job entries.`);

    // Firestore batch writes in chunks with retries
    let totalWrites = 0;
    for (let i = 0; i < normalized.length; i += FIRESTORE_BATCH_MAX) {
      const chunk = normalized.slice(i, i + FIRESTORE_BATCH_MAX);
      await retry(
        async () => {
          const batch = writeBatch(db);
          for (const job of chunk) {
            const docRef = firestoreDoc(db, 'jobs', job.id);
            batch.set(
              docRef,
              {
                title: job.title,
                company: job.company,
                location: job.location,
                date: job.date || null,
                rawDate: job.rawDate || null,
                url: job.url,
                scrapedAt: serverTimestamp(),
              },
              { merge: true } // keep existing metadata
            );
            totalWrites += 1;
          }
          await batch.commit();
        },
        {
          attempts: 3,
          baseDelay: 500,
          onRetry: (a, err) => warn(`Firestore batch commit retry ${a} due to: ${err.message}`),
        }
      );
      info(`Committed batch of up to ${FIRESTORE_BATCH_MAX} entries (iteration ${Math.floor(i / FIRESTORE_BATCH_MAX) + 1}).`);
    }

    info(`Firestore updates complete. Total writes: ${totalWrites}.`);

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    info(`Scraping finished in ${elapsed}s.`);

    if (process.env.SEND_SUMMARY !== 'false') {
      // best-effort notify
      sendTelegramMessage(`Scraper success: ${normalized.length} items, duration ${elapsed}s.`).catch(() => {});
    }

    return { success: true, processed: normalized.length };
  } catch (err) {
    error('Fatal error during scraping:', err && err.stack ? err.stack : err);
    try {
      await sendTelegramMessage(`Scraper failed: ${err && (err.message || err)}`);
    } catch (notifErr) {
      warn('Failed to send failure notification:', notifErr && (notifErr.message || notifErr));
    }
    return { success: false, error: err && (err.message || String(err)) };
  } finally {
    try {
      // close global browser if present
      if (globalThis.__PUPPETEER_BROWSER && typeof globalThis.__PUPPETEER_BROWSER.close === 'function') {
        await globalThis.__PUPPETEER_BROWSER.close().catch(() => {});
      }
      // local cleanup: close explicit browser reference if any
    } catch (cleanupErr) {
      warn('Error during cleanup:', cleanupErr && (cleanupErr.message || cleanupErr));
    }
  }
}

/* ----------------------------- Global Safety ----------------------------- */

process.on('unhandledRejection', (reason) => {
  error('Unhandled Rejection:', reason && (reason.stack || reason));
  sendTelegramMessage(`Unhandled Rejection: ${reason && (reason.message || reason)}`).finally(() =>
    process.exit(1)
  );
});

process.on('uncaughtException', (err) => {
  error('Uncaught Exception:', err && (err.stack || err));
  sendTelegramMessage(`Uncaught Exception: ${err && (err.message || err)}`).finally(() =>
    process.exit(1)
  );
});

/* ----------------------------- Entrypoint ----------------------------- */

if (process.argv[1] === __filename) {
  (async () => {
    try {
      const result = await runScraper();
      if (!result.success) process.exit(2);
      process.exit(0);
    } catch (err) {
      error('Unhandled top-level error:', err && (err.stack || err));
      await sendTelegramMessage(`Top-level error: ${err && (err.message || err)}`).catch(() => {});
      process.exit(1);
    }
  })();
}
