// index.js - Production Job Scraper - FINAL ROBUST VERSION (State Machine)

import { createHash } from 'crypto';
import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import { initializeApp, getApps } from 'firebase/app';
import { getAuth, signInWithCustomToken, signInAnonymously } from 'firebase/auth';
import { getFirestore, doc, getDoc, setDoc, serverTimestamp } from 'firebase/firestore';
import express from 'express';
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
const WEBSITE_PASS = process.env.WEBSITE_PASS;
const JOBS_PAGE_URL = process.env.JOBS_PAGE_URL;
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
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
        const configStr = process.env.__firebase_config;
        if (!configStr) {
            logger.error('Missing __firebase_config');
            return false;
        }
        const app = getApps().length === 0 ? initializeApp(JSON.parse(configStr)) : getApps()[0];
        auth = getAuth(app);
        db = getFirestore(app);

        const token = process.env.__initial_auth_token;
        if (token) await signInWithCustomToken(auth, token);
        else await signInAnonymously(auth);

        currentUserId = auth.currentUser.uid;
        logger.info(`[FIREBASE] Connected as ${currentUserId}`);
        return true;
    } catch (e) {
        logger.error(`[FIREBASE] Error: ${e.message}`);
        return false;
    }
}

// --- 3. UTILS ---
function createJobId(date, event, doctor) {
    // Create a unique ID based on the job details
    const raw = `${date}|${event}|${doctor}`.toLowerCase().replace(/\s+/g, ' ').trim();
    return createHash('md5').update(raw).digest('hex');
}

async function isJobNew(jobId) {
    if (!db || !currentUserId) throw new Error('Firebase not initialized');
    const docRef = doc(db, 'artifacts', process.env.__app_id || 'job-scraper-app', 'users', currentUserId, 'job_history', jobId);
    return !(await getDoc(docRef)).exists();
}

async function saveJobToHistory(jobId, job) {
    if (!db || !currentUserId) throw new Error('Firebase not initialized');
    const docRef = doc(db, 'artifacts', process.env.__app_id || 'job-scraper-app', 'users', currentUserId, 'job_history', jobId);
    await setDoc(docRef, { ...job, jobId, savedAt: serverTimestamp() });
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

// --- 5. CORE SCRAPER ---
async function mainScraper() {
    let browser = null;
    try {
        if (!WEBSITE_USERNAME || !WEBSITE_PASS) throw new Error("Missing credentials");
        if (!(await initializeFirebase())) throw new Error("Firebase failed");

        puppeteer.use(StealthPlugin());
        browser = await puppeteer.launch({
            headless: true,
            args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
        });

        const page = await browser.newPage();

        // Suppress benign "Failed to load resource" logs for cleaner output
        page.on('console', msg => {
            const text = msg.text();
            if (!text.includes('Failed to load resource')) {
                logger.info(`[BROWSER] ${text}`);
            }
        });

        // Aggressive Blocking to speed up scrape
        await page.setRequestInterception(true);
        page.on('request', req => {
            const type = req.resourceType();
            if (['image', 'font', 'stylesheet', 'media'].includes(type)) req.abort();
            else req.continue();
        });

        await page.setViewport({ width: 1920, height: 1080 });

        // Login
        logger.info(`[LOGIN] Navigating...`);
        await page.goto(LOGIN_URL, { waitUntil: 'networkidle0' });
        await humanDelay(1000);
        await page.type(SELECTORS.USERNAME_INPUT, WEBSITE_USERNAME);
        await page.type(SELECTORS.PASSWORD_INPUT, WEBSITE_PASS);

        await Promise.all([
            page.waitForNavigation({ waitUntil: 'networkidle0' }),
            page.click(SELECTORS.LOGIN_BUTTON),
        ]);

        if (page.url().includes('login.php')) throw new Error("Login failed");
        logger.info(`[LOGIN] Success`);

        // Scrape
        logger.info(`[SCRAPE] Loading jobs page...`);
        await page.goto(JOBS_PAGE_URL, { waitUntil: 'networkidle0', timeout: 60000 });
        await page.waitForSelector('table', { timeout: 30000 });

        // --- STRICT STATE MACHINE PARSING ---
        const jobs = await page.evaluate(() => {
            const results = [];

            // State Variables to carry context across rows
            let currentDate = 'Unknown Date';
            let currentEvent = 'Unknown Event';

            const rows = Array.from(document.querySelectorAll('table tr'));
            console.log(`Processing ${rows.length} rows for extraction...`);

            for (const row of rows) {
                // Get all text cells
                const cells = Array.from(row.querySelectorAll('td')).map(td => td.innerText.trim());
                if (cells.length === 0) continue;

                const col0 = cells[0]; // First cell content

                // --- DETECT ROW TYPE ---

                // Regex to match a day name at the start of the cell (e.g., 'Mon 25th Nov 2025')
                const isDateRow = /^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)/i.test(col0);
                // Regex to match a time range in the cell (e.g., '17:15 - 23:15')
                const isTimeRange = /\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2}/.test(col0);


                // TYPE 1: DATE ROW (Header Row)
                if (isDateRow && !isTimeRange) {
                    currentDate = col0;
                    currentEvent = 'Unknown Event'; // Reset event for the new date
                    continue; 
                }

                // TYPE 2: PARENT EVENT ROW (Row containing the Time, Event Name, and maybe the first job)
                if (isTimeRange) {
                    // This is a parent row (first cell is time). Event Name is in the second cell (index 1).
                    if (cells[1]) {
                        // Clean up the event name by removing any time ranges that might be stuck in it
                        currentEvent = cells[1]
                            .replace(/\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2}/g, '')
                            .trim();
                    }

                    // Check for Doctor in THIS row (Role is at index 2, Name at index 4)
                    if (cells.length > 2 && cells[2] === 'Doctor') {
                        const docName = (cells.length > 4) ? cells[4] : 'Unassigned';

                        if (currentEvent !== 'Unknown Event') {
                            console.log(`[FOUND-PARENT] ${currentDate} | ${currentEvent} | ${docName}`);
                            results.push({
                                date: currentDate,
                                eventName: currentEvent,
                                doctorName: docName,
                                jobName: 'Doctor'
                            });
                        }
                    }
                    continue; // Done with this row
                }

                // TYPE 3: CHILD JOB ROW (First cell is the Role, e.g., "Doctor")
                if (col0 === 'Doctor') {
                    // This is a child job row (inherits date/event from state)
                    // Col 0: Job Role
                    // Col 1: Time
                    // Col 2: Person Name (We extract this!)

                    const docName = (cells.length > 2) ? cells[2] : 'Unassigned';

                    if (currentDate !== 'Unknown Date' && currentEvent !== 'Unknown Event') {
                        console.log(`[FOUND-CHILD] ${currentDate} | ${currentEvent} | ${docName}`);
                        results.push({
                            date: currentDate,
                            eventName: currentEvent,
                            doctorName: docName,
                            jobName: 'Doctor'
                        });
                    }
                }
            }
            return results;
        });

        logger.info(`[SCRAPE] Found ${jobs.length} Doctor jobs.`);

        // Process & Filter
        const newJobs = [];
        for (const job of jobs) {
            // Only proceed if we have valid context
            if (job.date === 'Unknown Date' || job.eventName === 'Unknown Event') continue;

            const id = createJobId(job.date, job.eventName, job.doctorName);
            if (await isJobNew(id)) {
                newJobs.push(job);
                await saveJobToHistory(id, job);
            }
        }

        if (newJobs.length > 0) {
            const list = newJobs.map((j, i) => `${i + 1}. ${j.date}\n   ${j.eventName}\n   ${j.doctorName}`).join('\n\n');
            await sendTelegram(`🚨 *NEW DOCTOR JOBS*\n\n${list}\n\n[Link](${JOBS_PAGE_URL})`);
        }

        return { status: "success", new: newJobs.length, total: jobs.length, data: newJobs };

    } catch (e) {
        logger.error(`[CRITICAL] ${e.message}`);
        await sendTelegram(`⚠️ Scraper Failed: ${e.message}`);
        return { status: "error", message: e.message };
    } finally {
        if (browser) await browser.close();
    }
}

// --- 6. SERVER ---
const app = express();
const port = process.env.PORT || 3000;

app.get('/', (req, res) => res.send('Scraper Ready'));
app.get('/scrape', async (req, res) => {
    const result = await mainScraper();
    res.status(result.status === 'success' ? 200 : 500).json(result);
});

app.listen(port, () => logger.info(`[SERVER] Listening on ${port}`));