/**
 * index.js
 *
 * ES Module scraper using Puppeteer + Firebase client SDK (modular) + Telegram notifications.
 *
 * Key fixes in this version:
 * - Removed firebase-admin (server SDK) usage and replaced with client-side Firebase SDK:
 *   import { initializeApp } from 'firebase/app' and { getFirestore, writeBatch, doc, serverTimestamp } from 'firebase/firestore'.
 * - Uses FIREBASE_CONFIG (JSON string) or FIREBASE_CONFIG_PATH environment variable for client config.
 * - Avoids importing node-fetch directly; uses global fetch when available (Node 18+). If fetch is not available,
 *   Telegram notifications are skipped to avoid adding dependencies that may not be installed.
 * - Keeps robust error handling, retries, Firestore batch writes, and date-only extraction.
 *
 * Required environment variables:
 * - SCRAPE_URL
 * - FIREBASE_CONFIG (JSON string) OR FIREBASE_CONFIG_PATH (path to json file)
 * - FIREBASE_PROJECT_ID (optional but recommended)
 * - PUPPETEER_HEADLESS (optional, default "true")
 * - TELEGRAM_BOT_TOKEN (optional)
 * - TELEGRAM_CHAT_ID (optional)
 *
 * Note: Adjust selectors via env vars LIST_CONTAINER_SELECTOR and ITEM_ROW_SELECTOR as required.
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

// Basic logger helpers
const info = (...args) => console.info(new Date().toISOString(), '[INFO]', ...args);
const warn = (...args) => console.warn(new Date().toISOString(), '[WARN]', ...args);
const error = (...args) => console.error(new Date().toISOString(), '[ERROR]', ...args);

/* ----------------------------- Initialization ----------------------------- */

/**
 * Initialize Firebase client SDK using FIREBASE_CONFIG (stringified JSON) or FIREBASE_CONFIG_PATH.
 * Returns the initialized app and Firestore instance.
 */
function initFirebaseClient() {
  // Prefer FIREBASE_CONFIG env var (JSON string)
  let firebaseConfig = null;

  if (process.env.FIREBASE_CONFIG) {
    try {
      firebaseConfig = JSON.parse(process.env.FIREBASE_CONFIG);
    } catch (err) {
      throw new Error('FIREBASE_CONFIG is not valid JSON.');
    }
  } else if (process.env.FIREBASE_CONFIG_PATH) {
    const providedPath = process.env.FIREBASE_CONFIG_PATH;
    const resolved = path.isAbsolute(providedPath)
      ? providedPath
      : path.resolve(process.cwd(), providedPath);
    try {
      const raw = fs.readFileSync(resolved, 'utf8');
      firebaseConfig = JSON.parse(raw);
    } catch (err) {
      throw new Error(`Failed loading FIREBASE_CONFIG from path "${resolved}": ${err.message}`);
    }
  } else {
    throw new Error('Either FIREBASE_CONFIG (JSON string) or FIREBASE_CONFIG_PATH must be provided.');
  }

  // Initialize app (idempotent: multiple calls safe in most environments)
  const app = initializeApp(firebaseConfig);

  // Initialize Firestore
  const db = getFirestore(app);

  info('Initialized Firebase client SDK.');

  return { app, db };
}

/**
 * Send Telegram message using global fetch if available.
 * If fetch is not available in the runtime, the function will log a warning and return.
 */
async function sendTelegramMessage(text) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) {
    warn('Telegram not configured (TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing). Skipping notification.');
    return;
  }

  if (typeof globalThis.fetch !== 'function') {
    warn('global fetch is not available in this Node runtime. Skipping Telegram notification to avoid extra dependencies.');
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

/* ----------------------------- Helpers ----------------------------- */

/**
 * Sleep for ms milliseconds.
 */
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Retry wrapper with exponential backoff for async functions.
 */
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

/**
 * Wait for selector with small timeout and automatic retries.
 */
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
        warn(`Retry ${attempt}/${attempts} for selector "${selector}" after error: ${err.message}. Waiting ${delay}ms...`),
    }
  );
}

/* ----------------------------- Date Parsing / Formatting ----------------------------- */

/**
 * Given a raw text extracted from the "green row" date element, return a date-only string.
 *
 * Rules:
 * - Remove any explicit time components and timezone abbreviations.
 * - If the remaining text contains a month name (e.g., "Dec 20, 2025"), preserve that format.
 * - If relative ("today", "yesterday"), convert to YYYY-MM-DD.
 * - Otherwise standardize to YYYY-MM-DD when parseable.
 */
function extractDateOnly(rawText, now = new Date()) {
  if (!rawText || typeof rawText !== 'string') return null;
  let s = rawText.trim();

  // Normalize whitespace
  s = s.replace(/\s+/g, ' ');

  const lowered = s.toLowerCase();

  // Handle relative terms
  if (/\btoday\b/.test(lowered)) {
    return formatDateISO(new Date(now.getFullYear(), now.getMonth(), now.getDate()));
  }
  if (/\byesterday\b/.test(lowered)) {
    const d = new Date(now);
    d.setDate(d.getDate() - 1);
    return formatDateISO(new Date(d.getFullYear(), d.getMonth(), d.getDate()));
  }

  // Remove 'at' before time
  s = s.replace(/\bat\s+/i, ' ');

  // Remove timezone indicators like "EST", "PST", "GMT+1", "UTC", etc.
  s = s.replace(/\b(?:[A-Z]{2,5}|GMT[+\-]?\d{1,2}|UTC[+\-]?\d{1,2}|[A-Z]{1,4} ?GMT)\b/gi, '');

  // Remove time components: "12:30", "12:30:00", "12:30 PM", "12 PM"
  s = s.replace(/\b\d{1,2}:\d{2}(?::\d{2})?\s?(?:am|pm|AM|PM)?\b/g, '');
  s = s.replace(/\b\d{1,2}\s?(?:am|pm)\b/gi, '');
  s = s.replace(/\b\d{3,4}hrs?\b/gi, '').replace(/\b\d{2}:\d{2}:\d{2}\b/g, '');

  // Clean connectors and bullets
  s = s.replace(/\b(posted|posted on|posted:)\b/gi, '');
  s = s.replace(/[|–—•]/g, ' ');

  // Collapse whitespace & trim
  s = s.replace(/\s+/g, ' ').trim();

  // Preserve human-readable month formats if present
  const monthPattern = /\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b/i;
  if (monthPattern.test(s)) {
    const human = s.replace(/,\s*$/, '').trim();
    if (!/:\d{2}/.test(human)) {
      return human;
    }
  }

  // Try parse as a date
  const parsedDate = parseFlexibleDate(s);
  if (parsedDate) {
    return formatDateISO(parsedDate);
  }

  // Fallback for mm/dd or mm-dd (with optional year)
  const mmdd = s.match(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b/);
  if (mmdd) {
    const month = parseInt(mmdd[1], 10);
    const day = parseInt(mmdd[2], 10);
    const year = mmdd[3] ? parseInt(mmdd[3], 10) : now.getFullYear();
    if (month >= 1 && month <= 12 && day >= 1 && day <= 31) {
      return formatDateISO(new Date(year, month - 1, day));
    }
  }

  // Unable to extract a reliable date-only string
  return null;
}

function parseFlexibleDate(text) {
  if (!text || typeof text !== 'string') return null;
  const parsed = Date.parse(text);
  if (!Number.isNaN(parsed)) return new Date(parsed);

  // Try appending current year if missing
  const now = new Date();
  const withYear = `${text} ${now.getFullYear()}`;
  const parsed2 = Date.parse(withYear);
  if (!Number.isNaN(parsed2)) return new Date(parsed2);

  return null;
}

function formatDateISO(date) {
  if (!(date instanceof Date)) return null;
  const y = date.getFullYear();
  const m = (`0${date.getMonth() + 1}`).slice(-2);
  const d = (`0${date.getDate()}`).slice(-2);
  return `${y}-${m}-${d}`;
}

/* ----------------------------- Main Scraper ----------------------------- */

async function runScraper() {
  const startTime = Date.now();
  info('Scraper starting');

  // Initialize client-side Firebase
  const { db } = initFirebaseClient();

  const scrapeUrl = process.env.SCRAPE_URL;
  if (!scrapeUrl) {
    throw new Error('SCRAPE_URL environment variable is required.');
  }

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
        onRetry: (a, err) => warn(`Retrying page.goto (${a}) due to: ${err.message}`),
      }
    );

    const listContainerSelector = process.env.LIST_CONTAINER_SELECTOR || '.job-listing, .results, #results';
    try {
      await waitForSelectorWithRetries(page, listContainerSelector, { attempts: 2 });
    } catch (err) {
      warn(`List container not found with default selector (${listContainerSelector}). Will proceed, attempting best-effort scraping.`);
    }

    const jobs = await page.$$eval(
      process.env.ITEM_ROW_SELECTOR || '.job-row, .listing, article, tr',
      (nodes) => {
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
      }
    );

    info(`Scraped ${jobs.length} job entries from the page.`);

    // Normalize jobs and dedupe
    const normalized = [];
    const seen = new Set();
    for (const item of jobs) {
      if (!item.title && !item.company) continue;
      const idSource = item.url || item.idAttr || `${item.title || ''}::${item.company || ''}`;
      const id = crypto.createHash('sha256').update(idSource).digest('hex');

      if (seen.has(id)) continue;
      seen.add(id);

      const dateOnly = extractDateOnly(item.rawDate);
      const job = {
        id,
        title: item.title || null,
        company: item.company || null,
        location: item.location || null,
        rawDate: item.rawDate || null,
        date: dateOnly, // either human-readable like "Dec 20, 2025" or ISO YYYY-MM-DD or null
        url: item.url || null,
      };
      normalized.push(job);
    }

    info(`Normalized ${normalized.length} unique job entries.`);

    // Firestore batch writes using client SDK
    const BATCH_MAX = 400;
    const batches = [];
    for (let i = 0; i < normalized.length; i += BATCH_MAX) {
      batches.push(normalized.slice(i, i + BATCH_MAX));
    }

    let totalWrites = 0;
    for (const chunk of batches) {
      const batch = writeBatch(db);
      for (const job of chunk) {
        const docRef = firestoreDoc(db, 'jobs', job.id);
        // Use merge to avoid overwriting unrelated fields
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
          { merge: true }
        );
        totalWrites += 1;
      }

      // Commit with retry
      await retry(
        () => batch.commit(),
        {
          attempts: 3,
          baseDelay: 500,
          onRetry: (a, err) => warn(`Retrying batch.commit (${a}) due to: ${err.message}`),
        }
      );
      info(`Committed batch of ${chunk.length} jobs to Firestore.`);
    }

    info(`Completed Firestore updates. Total writes: ${totalWrites}.`);

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    info(`Scraping finished in ${elapsed} seconds.`);

    // Optionally send a summary Telegram message on success
    if (process.env.SEND_SUMMARY !== 'false') {
      // Send but don't await to avoid blocking finalization unnecessarily
      sendTelegramMessage(`Scraper finished successfully. ${normalized.length} jobs processed. Duration: ${elapsed}s.`).catch(() => {});
    }

    return { success: true, processed: normalized.length };
  } catch (err) {
    error('Fatal error during scraping:', err && err.stack ? err.stack : err);
    // Send a critical Telegram notification if possible
    try {
      await sendTelegramMessage(`Scraper failed: ${err.message || err}`);
    } catch (notifErr) {
      warn('Failed sending failure notification:', notifErr && (notifErr.message || notifErr));
    }
    return { success: false, error: err.message || String(err) };
  } finally {
    if (browser) {
      try {
        await browser.close();
        info('Browser closed.');
      } catch (err) {
        warn('Error closing browser:', err && (err.message || err));
      }
    }
  }
}

/* ----------------------------- Safety hooks ----------------------------- */

process.on('unhandledRejection', (reason) => {
  error('Unhandled Rejection:', reason && reason.stack ? reason.stack : reason);
  sendTelegramMessage(`Scraper unhandledRejection: ${reason && reason.message ? reason.message : reason}`).finally(() =>
    process.exit(1)
  );
});

process.on('uncaughtException', (err) => {
  error('Uncaught Exception:', err && err.stack ? err.stack : err);
  sendTelegramMessage(`Scraper uncaughtException: ${err && err.message ? err.message : err}`).finally(() =>
    process.exit(1)
  );
});

/* ----------------------------- Entrypoint ----------------------------- */

if (process.argv[1] === __filename) {
  (async () => {
    try {
      const result = await runScraper();
      if (!result.success) {
        process.exit(2);
      }
      process.exit(0);
    } catch (err) {
      error('Unhandled top-level error:', err && err.stack ? err.stack : err);
      await sendTelegramMessage(`Scraper top-level error: ${err && err.message ? err.message : err}`).catch(() => {});
      process.exit(1);
    }
  })();
}
