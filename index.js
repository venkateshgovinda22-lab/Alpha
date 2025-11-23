// index.js - Production Job Scraper - SERVERLESS VERSION (GitHub Actions Ready)

import { createHash } from 'crypto';
import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import { initializeApp, getApps } from 'firebase/app';
import { getAuth, signInWithCustomToken, signInAnonymously } from 'firebase/auth';
import { getFirestore, doc, getDoc, setDoc, serverTimestamp } from 'firebase/firestore';
import winston from 'winston';

// --- 1. CONFIGURATION & LOGGER ---
const SELECTORS = {
    USERNAME_INPUT: 'input[name="username"]',
    PASSWORD_INPUT: 'input[name="password"]',
    LOGIN_BUTTON: 'input[value="Login"]',
    JOB_TABLE: 'table', 
};

const LOGIN_URL = 'https://signups.org.uk/auth/login.php?xsi=12';
const WEBSITE_USERNAME = process.env.WEBSITE_USERNAME;
const WEBSITE_PASS = process.env.WEBSITE_PASSWORD; // Fixed variable name to match secrets
const JOBS_PAGE_URL = process.env.JOBS_PAGE_URL || 'https://signups.org.uk/index.php'; // Ensure this URL is correct
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_TOKEN; // Fixed variable name to match secrets
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
        winston.format.printf(({ level, message, timestamp }) => `[${timestamp}] ${level.toUpperCase()}: ${message}`)
    ),
    transports: [new winston.transports.Console()],
});

// --- 2. FIREBASE INIT ---
let db, auth, currentUserId;

async function initializeFirebase() {
    try {
        const configStr = process.env.FIREBASE_CONFIG;
        if (!configStr) {
            logger.error('Missing FIREBASE_CONFIG');
            return false;
        }
        const app = getApps().length === 0 ? initializeApp(JSON.parse(configStr)) : getApps()[0];
        auth = getAuth(app);
        db = getFirestore(app);

        // Auth fallback
        if (!auth.currentUser) {
             await signInAnonymously(auth);
        }
        currentUserId = auth.currentUser ? auth.currentUser.uid : 'anonymous';

        logger.info(`[FIREBASE] Connected as ${currentUserId}`);
        return true;
    } catch (e) {
        logger.error(`[FIREBASE] Error: ${e.message}`);
        return false;
    }
}

// --- 3. UTILS ---
function createJobId(date, event, doctor) {
    const raw = `${date}|${event}|${doctor}`.toLowerCase().replace(/\s+/g, ' ').trim();
    return createHash('md5').update(raw).digest('hex');
}

async function isJobNew(jobId) {
    if (!db) throw new Error('Firebase not initialized');
    // Ensure the path 'users/USER_ID/job_history' exists or use a global collection
    // Using a simpler path for stability if user ID varies:
    const docRef = doc(db, 'scraped_jobs', jobId); 
    return !(await getDoc(docRef)).exists();
}

async function saveJobToHistory(jobId, job) {
    if (!db) throw new Error('Firebase not initialized');
    const docRef = doc(db, 'scraped_jobs', jobId);
    await setDoc(docRef, { ...job, jobId, savedAt: serverTimestamp() });
}

const humanDelay = (ms) => new Promise(r => setTimeout(r, ms + Math.random() * 1000));

// --- 4. NOTIFICATIONS ---
async function sendTelegram(text) {
    if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
        logger.warn('[TELEGRAM] Tokens missing, skipping notification.');
        return;
    }
    try {
        await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                chat_id: TELEGRAM_CHAT_ID,
                text: text,
                parse_mode: 'Markdown',
                disable_web_page_preview: true
            })
        });
    } catch (e) {
        logger.error(`[TELEGRAM] Failed: ${e.message}`);
    }
}

// --- 5. CORE SCRAPER ---
async function mainScraper() {
    let browser = null;
    try {
        if (!WEBSITE_USERNAME || !WEBSITE_PASS) throw new Error("Missing website credentials");
        if (!(await initializeFirebase())) throw new Error("Firebase init failed");

        puppeteer.use(StealthPlugin());
        browser = await puppeteer.launch({
            headless: "new",
            args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
        });

        const page = await browser.newPage();

        // Console cleanup
        page.on('console', msg => {
            const text = msg.text();
            if (!text.includes('Failed to load resource')) {
                logger.info(`[BROWSER] ${text}`);
            }
        });

        await page.setViewport({ width: 1920, height: 1080 });

        // Login
        logger.info(`[LOGIN] Navigating to ${LOGIN_URL}...`);
        await page.goto(LOGIN_URL, { waitUntil: 'networkidle2', timeout: 60000 });
        
        await humanDelay(1000);
        await page.type(SELECTORS.USERNAME_INPUT, WEBSITE_USERNAME);
        await page.type(SELECTORS.PASSWORD_INPUT, WEBSITE_PASS);

        await Promise.all([
            page.waitForNavigation({ waitUntil: 'networkidle2' }),
            page.click(SELECTORS.LOGIN_BUTTON),
        ]);

        // Basic check if login redirected
        if (page.url().includes('login.php')) {
            throw new Error("Login failed - still on login page");
        }
        logger.info(`[LOGIN] Success`);

        // Scrape Jobs Page
        // Note: Use the variable JOBS_PAGE_URL or navigate naturally if needed
        const targetUrl = process.env.JOBS_PAGE_URL || 'https://signups.org.uk/index.php';
        logger.info(`[SCRAPE] Navigating to jobs page: ${targetUrl}`);
        
        await page.goto(targetUrl, { waitUntil: 'networkidle2', timeout: 60000 });
        
        // Wait for table or body
        try {
            await page.waitForSelector('table', { timeout: 15000 });
        } catch (e) {
            logger.warn('No table found, might be no jobs or different layout.');
        }

        // --- PARSING LOGIC ---
        const jobs = await page.evaluate(() => {
            const results = [];
            let currentDate = 'Unknown Date';
            let currentEvent = 'Unknown Event';

            const rows = Array.from(document.querySelectorAll('table tr'));
            
            for (const row of rows) {
                const cells = Array.from(row.querySelectorAll('td')).map(td => td.innerText.trim());
                if (cells.length === 0) continue;

                const col0 = cells[0];

                // Regex Checks
                const isDateRow = /^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)/i.test(col0);
                const isTimeRange = /\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2}/.test(col0);

                // 1. Header Row (Date)
                if (isDateRow && !isTimeRange) {
                    currentDate = col0;
                    currentEvent = 'Unknown Event';
                    continue;
                }

                // 2. Event Parent Row
                if (isTimeRange) {
                    if (cells[1]) {
                        currentEvent = cells[1].replace(/\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2}/g, '').trim();
                    }
                    // Check for immediate doctor assignment in this row
                    if (cells.length > 2 && cells[2] === 'Doctor') {
                        const docName = (cells.length > 4) ? cells[4] : 'Unassigned';
                        if (currentEvent !== 'Unknown Event') {
                            results.push({ date: currentDate, eventName: currentEvent, doctorName: docName });
                        }
                    }
                    continue;
                }

                // 3. Child Job Row
                if (col0 === 'Doctor') {
                    const docName = (cells.length > 2) ? cells[2] : 'Unassigned';
                    if (currentDate !== 'Unknown Date' && currentEvent !== 'Unknown Event') {
                        results.push({ date: currentDate, eventName: currentEvent, doctorName: docName });
                    }
                }
            }
            return results;
        });

        logger.info(`[SCRAPE] Found ${jobs.length} potential Doctor jobs.`);

        // Process & Filter
        const newJobs = [];
        for (const job of jobs) {
            if (job.date === 'Unknown Date' || job.eventName === 'Unknown Event') continue;
            // We only care about Unassigned jobs if looking for vacancies? 
            // Or all jobs? Assuming we notify on ANY new entry for now.
            
            const id = createJobId(job.date, job.eventName, job.doctorName);
            
            if (await isJobNew(id)) {
                newJobs.push(job);
                await saveJobToHistory(id, job);
            }
        }

        if (newJobs.length > 0) {
            const list = newJobs.map((j, i) => `${i + 1}. ${j.date}\n   ${j.eventName}\n   ${j.doctorName}`).join('\n\n');
            await sendTelegram(`🚨 *NEW DOCTOR JOBS FOUND*\n\n${list}`);
        } else {
            logger.info('[SCRAPE] No new unique jobs found.');
        }

        return { status: "success", new: newJobs.length };

    } catch (e) {
        logger.error(`[CRITICAL] ${e.message}`);
        await sendTelegram(`⚠️ Scraper Error: ${e.message}`);
        return { status: "error", message: e.message };
    } finally {
        if (browser) await browser.close();
    }
}

// --- 6. EXECUTION BLOCK (NO SERVER) ---
(async () => {
    logger.info('[START] Starting Hourly Scrape...');
    const result = await mainScraper();
    
    if (result.status === 'success') {
        logger.info('[EXIT] Scrape completed successfully.');
        process.exit(0);
    } else {
        logger.error('[EXIT] Scrape failed.');
        process.exit(1);
    }
})();
