// index.js - Industrial Grade | GitHub Actions Optimized

import { createHash } from 'crypto';
import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import { initializeApp, getApps } from 'firebase/app';
import { getAuth, signInAnonymously } from 'firebase/auth';
import { getFirestore, doc, getDoc, setDoc, serverTimestamp } from 'firebase/firestore';
import winston from 'winston';

// --- 1. CONFIGURATION ---
const SELECTORS = {
    USERNAME_INPUT: 'input[name="username"]',
    PASSWORD_INPUT: 'input[name="password"]',
    LOGIN_BUTTON: 'input[value="Login"]',
    JOB_TABLE: 'table', 
};

// Env Variables
const LOGIN_URL = 'https://signups.org.uk/auth/login.php?xsi=12';
const WEBSITE_USERNAME = process.env.WEBSITE_USERNAME;
const WEBSITE_PASS = process.env.WEBSITE_PASSWORD; 
const JOBS_PAGE_URL = process.env.JOBS_PAGE_URL || 'https://signups.org.uk/areas/events/overview.php?settings=1&xsi=12';
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_TOKEN; 
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const TARGET_ROLE = 'Doctor';

const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
        winston.format.printf(({ level, message, timestamp }) => `[${timestamp}] ${level.toUpperCase()}: ${message}`)
    ),
    transports: [new winston.transports.Console()],
});

// --- 2. FIREBASE SETUP ---
let db;

async function initializeFirebase() {
    try {
        const configStr = process.env.FIREBASE_CONFIG;
        if (!configStr) {
            logger.error('Missing FIREBASE_CONFIG');
            return false;
        }
        const app = getApps().length === 0 ? initializeApp(JSON.parse(configStr)) : getApps()[0];
        const auth = getAuth(app);
        db = getFirestore(app);

        if (!auth.currentUser) {
            await signInAnonymously(auth);
        }
        logger.info(`[FIREBASE] Connected.`);
        return true;
    } catch (e) {
        logger.error(`[FIREBASE] Error: ${e.message}`);
        return false;
    }
}

// --- 3. UTILITY FUNCTIONS ---
function createJobId(date, event, doctor) {
    const raw = `${date}|${event}|${doctor}`.toLowerCase().replace(/\s+/g, ' ').trim();
    return createHash('md5').update(raw).digest('hex');
}

async function isJobNew(jobId) {
    const docRef = doc(db, 'scraped_doctor_jobs', jobId); 
    return !(await getDoc(docRef)).exists();
}

async function saveJobToHistory(jobId, job) {
    const docRef = doc(db, 'scraped_doctor_jobs', jobId);
    await setDoc(docRef, { 
        ...job, 
        jobId, 
        savedAt: serverTimestamp(),
        isVacancy: job.doctorName.toLowerCase().includes('unassigned') 
    });
}

const humanDelay = (ms) => new Promise(r => setTimeout(r, ms + Math.random() * 1000));

// --- 4. NOTIFICATIONS ---
async function sendTelegram(text) {
    if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
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

// --- 5. CORE SCRAPER LOGIC ---
async function mainScraper() {
    let browser = null;
    try {
        if (!WEBSITE_USERNAME || !WEBSITE_PASS) throw new Error("Missing credentials");
        if (!(await initializeFirebase())) throw new Error("Firebase init failed");

        puppeteer.use(StealthPlugin());
        browser = await puppeteer.launch({
            headless: "new",
            args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
        });

        const page = await browser.newPage();
        await page.setViewport({ width: 1920, height: 1080 });

        // --- LOGIN STEP ---
        logger.info(`[LOGIN] Starting fresh login sequence...`);
        await page.goto(LOGIN_URL, { waitUntil: 'networkidle2', timeout: 60000 });
        await humanDelay(1500); // Human pause
        
        await page.type(SELECTORS.USERNAME_INPUT, WEBSITE_USERNAME, { delay: 100 }); 
        await page.type(SELECTORS.PASSWORD_INPUT, WEBSITE_PASS, { delay: 100 });

        await Promise.all([
            page.waitForNavigation({ waitUntil: 'networkidle2' }),
            page.click(SELECTORS.LOGIN_BUTTON),
        ]);
        logger.info(`[LOGIN] Submitted.`);

        // --- SCRAPE STEP ---
        logger.info(`[SCRAPE] Checking jobs page...`);
        await page.goto(JOBS_PAGE_URL, { waitUntil: 'networkidle2', timeout: 60000 });
        
        try {
            await page.waitForSelector(SELECTORS.JOB_TABLE, { timeout: 15000 });
        } catch (e) {
             logger.warn('Job table not found (Page load issue or layout change).');
             return { status: "success", new: 0 };
        }

        const jobs = await page.evaluate((targetRole) => {
            const results = [];
            let currentDate = 'Unknown Date';
            let currentEvent = 'Unknown Event';
            const isDateRow = (text) => /^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)/i.test(text);
            const isTimeRange = (text) => /\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2}/.test(text);
            
            const rows = Array.from(document.querySelectorAll('table tr'));
            for (const row of rows) {
                const cells = Array.from(row.querySelectorAll('td')).map(td => td.innerText.trim());
                if (cells.length === 0) continue;
                const col0 = cells[0];

                if (isDateRow(col0) && !isTimeRange(col0)) {
                    currentDate = col0;
                    currentEvent = 'Unknown Event';
                    continue;
                }
                if (isTimeRange(col0)) {
                    if (cells.length > 1) {
                        currentEvent = cells[1].replace(/\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2}/g, '').trim() || cells[1].trim();
                        if (cells.length > 2 && cells[2] === targetRole) {
                             const docName = (cells.length > 4) ? cells[4] : 'Unassigned';
                             if (currentEvent !== 'Unknown Event') results.push({ date: currentDate, eventName: currentEvent, doctorName: docName });
                        }
                    }
                    continue;
                }
                if (col0 === targetRole) {
                    const docName = (cells.length > 2) ? cells[2] : 'Unassigned';
                    if (currentDate !== 'Unknown Date' && currentEvent !== 'Unknown Event') results.push({ date: currentDate, eventName: currentEvent, doctorName: docName });
                }
            }
            return results;
        }, TARGET_ROLE);

        logger.info(`[SCRAPE] Found ${jobs.length} potential '${TARGET_ROLE}' jobs.`);

        const newJobs = [];
        for (const job of jobs) {
            if (job.date === 'Unknown Date') continue;
            const id = createJobId(job.date, job.eventName, job.doctorName);
            if (await isJobNew(id)) {
                newJobs.push(job);
                await saveJobToHistory(id, job);
            }
        }

        if (newJobs.length > 0) {
            newJobs.sort((a, b) => {
                const aVacant = a.doctorName.toLowerCase().includes('unassigned');
                const bVacant = b.doctorName.toLowerCase().includes('unassigned');
                return bVacant - aVacant;
            });
            
            const list = newJobs.map((j) => {
                const isVacant = j.doctorName.toLowerCase().includes('unassigned') || j.doctorName === '';
                const icon = isVacant ? '🟢' : '🔴';
                const status = isVacant ? '**AVAILABLE / UNASSIGNED**' : `Taken by ${j.doctorName}`;
                return `${icon} *${j.date}*\n   ${j.eventName}\n   Status: ${status}`;
            }).join('\n\n');
            
            await sendTelegram(`🚨 *NEW UPDATES FOUND* (${newJobs.length})\n\n${list}\n\n[View Jobs](${JOBS_PAGE_URL})`);
            logger.info(`[NOTIFY] Sent alert.`);
        } else {
            logger.info('[SCRAPE] No new unique jobs.');
        }
        return { status: "success", new: newJobs.length };

    } catch (e) {
        logger.error(`[CRITICAL] ${e.message}`);
        await sendTelegram(`⚠️ Error: ${e.message}`);
        return { status: "error", message: e.message };
    } finally {
        if (browser) await browser.close();
    }
}

// --- 6. STEALTH STARTUP (JITTER) ---
(async () => {
    // Random wait between 0 and 4 minutes (240 seconds)
    // This protects against "exact hour" bot detection patterns
    const jitterSeconds = Math.floor(Math.random() * 240); 
    logger.info(`[STEALTH] Jitter delay active: Starting in ${jitterSeconds}s...`);
    await humanDelay(jitterSeconds * 1000);
    
    await mainScraper();
    process.exit(0);
})();
