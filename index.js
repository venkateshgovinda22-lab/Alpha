/**
 * index.js
 *
 * Industrial-grade refactor of the original Node.js scraper.
 *
 * Features & improvements:
 * - Granular error handling around Puppeteer operations with retry/backoff helper.
 * - Fast selectors with conservative timeouts to keep within CI 10-minute limit.
 * - Firestore batch writes to minimize network overhead and ensure atomic commits.
 * - Robust date extraction that strips any time component and preserves human-readable
 *   month/day formats where possible, otherwise standardizes to YYYY-MM-DD.
 * - Clear logging (console.info / console.warn / console.error).
 * - Telegram notifications for fatal errors or important summaries.
 * - Safe Firebase initialization using either a JSON string in env or a file path.
 * - Graceful shutdown and resource cleanup.
 *
 * Required environment variables:
 * - SCRAPE_URL               (the page to scrape)
 * - PUPPETEER_HEADLESS      (optional, default "true")
 * - FIREBASE_SERVICE_ACCOUNT (JSON string) OR FIREBASE_SERVICE_ACCOUNT_PATH (path to json file)
 * - FIREBASE_PROJECT_ID
 * - TELEGRAM_BOT_TOKEN      (optional, to send notifications)
 * - TELEGRAM_CHAT_ID        (optional)
 *
 * Note: Adjust selectors and extraction logic below to match the site being scraped.
 */

const puppeteer = require('puppeteer');
const admin = require('firebase-admin');
const crypto = require('crypto');
const fetch = require('node-fetch');

const DEFAULT_SELECTOR_TIMEOUT = 8_000; // ms - keep small so overall run is fast
const RETRY_ATTEMPTS = 3;
const RETRY_BASE_DELAY = 500; // ms

// Basic logger helpers
const info = (...args) => console.info(new Date().toISOString(), '[INFO]', ...args);
const warn = (...args) => console.warn(new Date().toISOString(), '[WARN]', ...args);
const error = (...args) => console.error(new Date().toISOString(), '[ERROR]', ...args);

/* ----------------------------- Initialization ----------------------------- */

function initFirebase() {
  if (admin.apps && admin.apps.length) {
    return admin.app();
  }

  const projectId = process.env.FIREBASE_PROJECT_ID;
  if (!projectId) {
    throw new Error('FIREBASE_PROJECT_ID environment variable is required.');
  }

  let serviceAccount;
  if (process.env.FIREBASE_SERVICE_ACCOUNT) {
    try {
      serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
    } catch (err) {
      throw new Error('FIREBASE_SERVICE_ACCOUNT is not valid JSON.');
    }
  } else if (process.env.FIREBASE_SERVICE_ACCOUNT_PATH) {
    // load from path
    // eslint-disable-next-line security/detect-non-literal-fs-filename
    serviceAccount = require(process.env.FIREBASE_SERVICE_ACCOUNT_PATH);
  } else {
    throw new Error('Either FIREBASE_SERVICE_ACCOUNT or FIREBASE_SERVICE_ACCOUNT_PATH must be provided.');
  }

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId,
  });

  info('Initialized Firebase Admin SDK for project', projectId);
  return admin.app();
}

async function sendTelegramMessage(text) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) {
    warn('Telegram not configured (TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing). Skipping notification.');
    return;
  }

  const url = `https://api.telegram.org/bot${token}/sendMessage`;
  try {
    const res = await fetch(url, {
      method: 'POST',
      body: JSON.stringify({ chat_id: chatId, text }),
      headers: { 'Content-Type': 'application/json' },
      // keep short timeout - don't let notif block the scraper
      // Note: node-fetch doesn't have built-in timeout in v2; rely on Github Action network constraints
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) {
      warn('Failed to send Telegram message:', res.status, body);
    } else {
      info('Telegram notification sent.');
    }
  } catch (err) {
    warn('Error sending Telegram message:', err.message || err);
  }
}

/* ----------------------------- Helpers ----------------------------- */

/**
 * Sleep for ms milliseconds.
 * Non-blocking.
 */
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Retry wrapper with exponential backoff for async functions.
 * Attempts fn up to attempts times with baseDelay * 2^(i) backoff.
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
 * Returns the ElementHandle or throws.
 */
async function waitForSelectorWithRetries(page, selector, opts = {}) {
  const timeout = opts.timeout ?? DEFAULT_SELECTOR_TIMEOUT;
  const attempts = opts.attempts ?? RETRY_ATTEMPTS;

  return retry(
    async () => {
      // Use page.waitForSelector with explicit small timeout
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
 * - Remove any explicit time components (e.g., "12:30 PM", "12:30", "12:30:00", "at 12:30pm").
 * - Remove timezone abbreviations ("EST", "GMT", "UTC+1", etc).
 * - If the remaining text clearly contains a month name and day (e.g., "Dec 20, 2025"), preserve that.
 * - If the text is relative ("today", "yesterday"), convert to YYYY-MM-DD.
 * - If no clear human-readable format is preserved, standardize to YYYY-MM-DD.
 */
function extractDateOnly(rawText, now = new Date()) {
  if (!rawText || typeof rawText !== 'string') return null;
  let s = rawText.trim();

  // Normalize common words
  s = s.replace(/\s+/g, ' ');

  // Lowercase temporary for checks
  const lowered = s.toLowerCase();

  // Handle "today" and "yesterday" (and variants)
  if (/\btoday\b/.test(lowered)) {
    return formatDateISO(new Date(now.getFullYear(), now.getMonth(), now.getDate()));
  }
  if (/\byesterday\b/.test(lowered)) {
    const d = new Date(now);
    d.setDate(d.getDate() - 1);
    return formatDateISO(new Date(d.getFullYear(), d.getMonth(), d.getDate()));
  }

  // Remove 'at' before time, e.g., "Dec 20, 2025 at 12:30 PM"
  s = s.replace(/\bat\s+/i, ' ');

  // Remove timezone indicators like "EST", "PST", "GMT+1", "UTC", "(GMT)", "IST", "CET"
  s = s.replace(/\b(?:[A-Z]{2,5}|GMT[+-]?\d{1,2}|UTC[+-]?\d{1,2}|[A-Z]{1,4} ?GMT)\b/gi, '');

  // Remove time components: 12:30, 12:30:00, 12:30 PM, 12:30AM, 23:59, etc.
  s = s.replace(/\b\d{1,2}:\d{2}(?::\d{2})?\s?(?:am|pm|AM|PM)?\b/g, '');
  // Remove standalone times like "12 PM" or "5am"
  s = s.replace(/\b\d{1,2}\s?(?:am|pm)\b/gi, '');
  // Remove 24-hour times like "2300hrs" or "2300"
  s = s.replace(/\b\d{3,4}hrs?\b/gi, '').replace(/\b\d{2}:\d{2}:\d{2}\b/g, '');

  // Common connectors that may remain (commas, 'posted', 'on')
  s = s.replace(/\b(posted|posted on|posted:)\b/gi, '');
  s = s.replace(/[|–—•]/g, ' ');

  // Collapse whitespace & trim
  s = s.replace(/\s+/g, ' ').trim();

  // If string contains month names or abbreviations, preserve human readable format
  const monthPattern = /\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b/i;
  if (monthPattern.test(s)) {
    // Some sites include extra separators like "Dec 20, 2025," - clean trailing commas
    const human = s.replace(/,\s*$/, '').trim();
    // Ensure we removed all time pieces -- double-check there's no digits with colon
    if (!/:\d{2}/.test(human)) {
      return human;
    }
  }

  // Try to parse remaining string as a date - if parseable, return YYYY-MM-DD
  // Accept formats like "2025-12-20", "12/20/2025", "20 Dec 2025"
  const parsedDate = parseFlexibleDate(s);
  if (parsedDate) {
    return formatDateISO(parsedDate);
  }

  // As a last resort, if we still have a compact date-like token (e.g., "12/20"), try to interpret as mm/dd with current year
  const mmdd = s.match(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?\b/);
  if (mmdd) {
    const month = parseInt(mmdd[1], 10);
    const day = parseInt(mmdd[2], 10);
    const year = mmdd[3] ? parseInt(mmdd[3], 10) : now.getFullYear();
    if (month >= 1 && month <= 12 && day >= 1 && day <= 31) {
      return formatDateISO(new Date(year, month - 1, day));
    }
  }

  // If nothing else, return null indicating we couldn't reliably extract a date-only string.
  return null;
}

/**
 * Try parsing flexible human-readable dates using built-in Date parsing and some fallbacks.
 * Returns Date object or null.
 */
function parseFlexibleDate(text) {
  if (!text || typeof text !== 'string') return null;
  // Attempt direct Date parsing
  const parsed = Date.parse(text);
  if (!Number.isNaN(parsed)) return new Date(parsed);

  // Try adding current year if missing and string like "Dec 20"
  const now = new Date();
  const withYear = `${text} ${now.getFullYear()}`;
  const parsed2 = Date.parse(withYear);
  if (!Number.isNaN(parsed2)) return new Date(parsed2);

  return null;
}

/**
 * Format Date to YYYY-MM-DD
 */
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

  const app = initFirebase();
  const db = admin.firestore();

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
    // Keep default navigation timeout reasonable
    page.setDefaultNavigationTimeout(60_000);

    info('Navigating to', scrapeUrl);
    await retry(
      () => page.goto(scrapeUrl, { waitUntil: ['domcontentloaded', 'networkidle2'], timeout: 60_000 }),
      {
        attempts: 3,
        baseDelay: 1000,
        onRetry: (a, err) => warn(`Retrying page.goto (${a}) due to: ${err.message}`),
      }
    );

    // Example: Wait for the main list container - adjust selector to your page.
    const listContainerSelector = process.env.LIST_CONTAINER_SELECTOR || '.job-listing, .results, #results';
    let containerHandle;
    try {
      containerHandle = await waitForSelectorWithRetries(page, listContainerSelector, { attempts: 2 });
    } catch (err) {
      warn(`List container not found with default selector (${listContainerSelector}). Will proceed, attempting best-effort scraping.`);
    }

    // Extract job entries from the page using a page.evaluate to avoid round-trips.
    // The actual selectors below should be adjusted to the site being scraped.
    const jobs = await page.$$eval(
      // Candidate row selector - try multiple options, fallback to 'article' or 'tr'
      process.env.ITEM_ROW_SELECTOR || '.job-row, .listing, article, tr',
      (nodes) => {
        const results = [];
        for (const node of nodes) {
          try {
            // Attempt to extract common fields; each selector attempt should be tolerant
            const titleEl = node.querySelector('.title, .job-title, h2, a') || null;
            const companyEl = node.querySelector('.company, .employer') || null;
            const locationEl = node.querySelector('.location') || null;
            // The "green row" date element may be marked with a class like 'date', 'posted', or have style
            const dateEl = node.querySelector('.date, .posted, .post-date, .green-row') || null;
            const linkEl = node.querySelector('a[href]') || null;

            const title = titleEl ? titleEl.innerText.trim() : null;
            const company = companyEl ? companyEl.innerText.trim() : null;
            const location = locationEl ? locationEl.innerText.trim() : null;
            const rawDate = dateEl ? dateEl.innerText.trim() : null;
            const url = linkEl ? linkEl.href : null;

            // Generate an id candidate - prefer an attribute if present
            const idAttr = node.getAttribute('data-id') || node.id || url || (title ? title.slice(0, 80) : null);
            results.push({ title, company, location, rawDate, url, idAttr });
          } catch (e) {
            // Ignore nodes that fail extraction
          }
        }
        return results;
      }
    );

    info(`Scraped ${jobs.length} job entries from the page.`);

    // Normalize jobs and remove duplicates
    const normalized = [];
    const seen = new Set();
    for (const item of jobs) {
      // Basic validation
      if (!item.title && !item.company) continue;

      // Create deterministic ID from url or idAttr or hashed title+company
      const idSource = item.url || item.idAttr || `${item.title || ''}::${item.company || ''}`;
      const id = crypto.createHash('sha256').update(idSource).digest('hex');

      if (seen.has(id)) continue;
      seen.add(id);

      // Fix date - remove any time component
      const dateOnly = extractDateOnly(item.rawDate);
      // If we couldn't extract any date, leave null to avoid inserting incorrect times
      const job = {
        id,
        title: item.title || null,
        company: item.company || null,
        location: item.location || null,
        rawDate: item.rawDate || null,
        date: dateOnly, // dateOnly is either human-readable like "Dec 20, 2025" or YYYY-MM-DD or null
        url: item.url || null,
      };
      normalized.push(job);
    }

    info(`Normalized ${normalized.length} unique job entries.`);

    // Firestore batch writes - commit in chunks of 400 (safely under 500 limit)
    const BATCH_MAX = 400;
    const batches = [];
    for (let i = 0; i < normalized.length; i += BATCH_MAX) {
      batches.push(normalized.slice(i, i + BATCH_MAX));
    }

    let totalWrites = 0;
    for (const chunk of batches) {
      const batch = db.batch();
      for (const job of chunk) {
        const docRef = db.collection('jobs').doc(job.id);
        // Only write the fields we want. Use merge:true to avoid overwriting unrelated metadata.
        batch.set(
          docRef,
          {
            title: job.title,
            company: job.company,
            location: job.location,
            date: job.date || null,
            rawDate: job.rawDate || null,
            url: job.url,
            scrapedAt: admin.firestore.FieldValue.serverTimestamp(),
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
      sendTelegramMessage(`Scraper finished successfully. ${normalized.length} jobs processed. Duration: ${elapsed}s.`);
    }

    return { success: true, processed: normalized.length };
  } catch (err) {
    error('Fatal error during scraping:', err && err.stack ? err.stack : err);
    // Send a critical Telegram notification
    try {
      await sendTelegramMessage(`Scraper failed: ${err.message || err}`);
    } catch (notifErr) {
      warn('Failed sending failure notification:', notifErr && notifErr.message ? notifErr.message : notifErr);
    }
    return { success: false, error: err.message || String(err) };
  } finally {
    if (browser) {
      try {
        await browser.close();
        info('Browser closed.');
      } catch (err) {
        warn('Error closing browser:', err && err.message ? err.message : err);
      }
    }
  }
}

/* ----------------------------- Safety hooks ----------------------------- */

// Global handlers to prevent process crashes without cleanup
process.on('unhandledRejection', (reason) => {
  error('Unhandled Rejection:', reason && reason.stack ? reason.stack : reason);
  // Attempt to notify then exit
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

if (require.main === module) {
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
