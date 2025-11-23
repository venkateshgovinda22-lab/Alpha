// index.js - Production Doctor Job Scraper - SERVERLESS VERSION (GitHub Actions Ready)

import { createHash } from 'crypto';
import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import { initializeApp, getApps } from 'firebase/app';
import { getAuth, signInAnonymously } from 'firebase/auth';
import { getFirestore, doc, getDoc, setDoc, serverTimestamp } from 'firebase/firestore';
import winston from 'winston';

// --- 1. CONFIGURATION & LOGGER ---
const SELECTORS = {
    USERNAME_INPUT: 'input[name="username"]',
    PASSWORD_INPUT: 'input[name="password"]',
    LOGIN_BUTTON: 'input[value="Login"]',
    JOB_TABLE: 'table', // Primary target selector
};

// Environment Variables - Ensure these are set in GitHub Secrets!
const LOGIN_URL = 'https://signups.org.uk/auth/login.php?xsi=12';
const WEBSITE_USERNAME = process.env.WEBSITE_USERNAME;
const WEBSITE_PASS = process.env.WEBSITE_PASSWORD; 
const JOBS_PAGE_URL = process.env.JOBS_PAGE_URL || 'https://signups.org.uk/areas/events/overview.php?settings=1&xsi=12';
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_TOKEN; 
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

// Define the specific job role we are interested in.
const TARGET_ROLE = 'Doctor';
// Set this to true if you only want notifications for VACANT jobs (DoctorName === 'Unassigned')
const ONLY_NOTIFY_VACANCIES = false; 

const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
        winston.format.printf(({ level, message, timestamp }) => `[${timestamp}] ${level.toUpperCase()}: ${message}`)
    ),
    transports: [new winston.transports.Console()],
});

// --- 2. FIREBASE INIT ---
let db;

/**
 * Initializes Firebase App and Firestore.
 * @returns {Promise<boolean>} True if initialized successfully.
 */
async function initializeFirebase() {
    try {
        const configStr = process.env.FIREBASE_CONFIG;
        if (!configStr) {
            logger.error('Missing FIREBASE_CONFIG');
            return false;
        }
        
        // Use existing app if it exists
        const app = getApps().length === 0 ? initializeApp(JSON.parse(configStr)) : getApps()[0];
        const auth = getAuth(app);
        db = getFirestore(app);

        // Auth fallback - use anonymous sign-in for firestore read/write
        if (!auth.currentUser) {
            await signInAnonymously(auth);
        }
        const currentUserId = auth.currentUser ? auth.currentUser.uid : 'anonymous';

        logger.info(`[FIREBASE] Connected as ${currentUserId}`);
        return true;
    } catch (e) {
        logger.error(`[FIREBASE] Error: ${e.message}`);
        return false;
    }
}

// --- 3. UTILS (Job History) ---
/**
 * Creates a stable, unique ID for a job entry.
 * @param {string} date - The date of the job.
 * @param {string} event - The name of the event/shift.
 * @param {string} doctor - The name of the assigned doctor (or 'Unassigned').
 * @returns {string} The MD5 hash ID.
 */
function createJobId(date, event, doctor) {
    const raw = `${date}|${event}|${doctor}`.toLowerCase().replace(/\s+/g, ' ').trim();
    return createHash('md5').update(raw).digest('hex');
}

/**
 * Checks if a job ID exists in the history collection.
 * @param {string} jobId - The job's unique ID.
 * @returns {Promise<boolean>} True if the job is new.
 */
async function isJobNew(jobId) {
    if (!db) throw new Error('Firebase not initialized');
    // Use a top-level collection for simplicity and stability
    const docRef = doc(db, 'scraped_doctor_jobs', jobId); 
    return !(await getDoc(docRef)).exists();
}

/**
 * Saves a new job entry to the history collection.
 * @param {string} jobId - The job's unique ID.
 * @param {object} job - The job data object.
 */
async function saveJobToHistory(jobId, job) {
    if (!db) throw new Error('Firebase not initialized');
    const docRef = doc(db, 'scraped_doctor_jobs', jobId);
    await setDoc(docRef, { 
        ...job, 
        jobId, 
        savedAt: serverTimestamp(),
        // Add a clean flag for vacancy status
        isVacancy: job.doctorName.toLowerCase().includes('unassigned') 
    });
}

/**
 * Creates a slight, human-like delay.
 * @param {number} ms - Base delay in milliseconds.
 */
const humanDelay = (ms) => new Promise(r => setTimeout(r, ms + Math.random() * 1000));

// --- 4. NOTIFICATIONS ---
/**
 * Sends a message to the configured Telegram chat.
 * @param {string} text - The message text.
 */
async function sendTelegram(text) {
    if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
        logger.warn('[TELEGRAM] Tokens missing, skipping notification.');
        return;
    }
    try {
        const response = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                chat_id: TELEGRAM_CHAT_ID,
                text: text,
                parse_mode: 'Markdown',
                disable_web_page_preview: true
            })
        });

        if (!response.ok) {
            const errorBody = await response.text();
            throw new Error(`Telegram API failed: ${response.status} - ${errorBody}`);
        }

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
            // Use 'new' for headless mode, essential for GitHub Actions
            headless: "new", 
            args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
        });

        const page = await browser.newPage();
        await page.setViewport({ width: 1920, height: 1080 });

        // --- LOGIN PROCESS ---
        logger.info(`[LOGIN] Navigating to ${LOGIN_URL}...`);
        await page.goto(LOGIN_URL, { waitUntil: 'networkidle2', timeout: 60000 });
        
        await humanDelay(1000);
        await page.type(SELECTORS.USERNAME_INPUT, WEBSITE_USERNAME);
        await page.type(SELECTORS.PASSWORD_INPUT, WEBSITE_PASS);

        await Promise.all([
            page.waitForNavigation({ waitUntil: 'networkidle2' }),
            page.click(SELECTORS.LOGIN_BUTTON),
        ]);

        if (page.url().includes('login.php')) {
            throw new Error("Login failed - still on login page. Check credentials.");
        }
        logger.info(`[LOGIN] Success`);

        // --- SCRAPE JOBS PAGE ---
        logger.info(`[SCRAPE] Navigating to jobs page: ${JOBS_PAGE_URL}`);
        await page.goto(JOBS_PAGE_URL, { waitUntil: 'networkidle2', timeout: 60000 });
        
        // Wait for the table to ensure content is loaded
        try {
            await page.waitForSelector(SELECTORS.JOB_TABLE, { timeout: 15000 });
        } catch (e) {
             logger.warn('Job table not found on page. Ending scrape.');
             return { status: "success", new: 0 };
        }

        // --- PARSING LOGIC (Client-Side) ---
        // This is where the core logic is now more robust.
        const jobs = await page.evaluate((targetRole) => {
            const results = [];
            let currentDate = 'Unknown Date';
            let currentEvent = 'Unknown Event';

            // Function to check if a string contains a date format (Mon, Tue, etc.)
            const isDateRow = (text) => /^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)/i.test(text);
            // Function to check for time range (e.g., 09:00 - 17:00)
            const isTimeRange = (text) => /\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2}/.test(text);

            const rows = Array.from(document.querySelectorAll('table tr'));
            
            for (const row of rows) {
                // Get all cells and trim whitespace. Use innerText for visible text.
                const cells = Array.from(row.querySelectorAll('td')).map(td => td.innerText.trim());
                if (cells.length === 0) continue;

                const col0 = cells[0];
                
                // 1. Header Row (Date)
                if (isDateRow(col0) && !isTimeRange(col0)) {
                    currentDate = col0;
                    currentEvent = 'Unknown Event'; // Reset event when a new date is found
                    continue;
                }

                // 2. Event Parent Row (A row that contains the time range and event name)
                if (isTimeRange(col0)) {
                    if (cells.length > 1) {
                        // The event name is usually in cell[1] but might contain the time range if it's not split
                        // Safely extract the event name by removing the time range.
                        currentEvent = cells[1].replace(/\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2}/g, '').trim() || cells[1].trim();
                        
                        // Check for 'Doctor' job assignment within this parent row structure (Cells[2] == Role)
                        if (cells.length > 2 && cells[2] === targetRole) {
                             const docName = (cells.length > 4) ? cells[4] : 'Unassigned';
                             if (currentEvent !== 'Unknown Event') {
                                 results.push({ date: currentDate, eventName: currentEvent, doctorName: docName });
                             }
                        }
                    }
                    continue;
                }

                // 3. Child Job Row (A row that starts with the Role name, e.g., 'Doctor')
                // This captures subsequent role rows related to the last 'currentEvent'
                if (col0 === targetRole) {
                    // Doctor name is expected in cell[2] for child rows
                    const docName = (cells.length > 2) ? cells[2] : 'Unassigned';
                    if (currentDate !== 'Unknown Date' && currentEvent !== 'Unknown Event') {
                        results.push({ date: currentDate, eventName: currentEvent, doctorName: docName });
                    }
                }
            }
            return results;
        }, TARGET_ROLE); // Pass TARGET_ROLE into the evaluate context

        logger.info(`[SCRAPE] Found ${jobs.length} potential '${TARGET_ROLE}' jobs.`);

        // --- PROCESSING & NOTIFICATION ---
        const newJobs = [];
        
        // 1. Filter for vacancies if requested
        const relevantJobs = ONLY_NOTIFY_VACANCIES
            ? jobs.filter(job => job.doctorName.toLowerCase().includes('unassigned'))
            : jobs;

        logger.info(`[SCRAPE] ${relevantJobs.length} relevant jobs for history check (Vacant only: ${ONLY_NOTIFY_VACANCIES}).`);

        // 2. Check history and save new jobs
        for (const job of relevantJobs) {
            // Ensure data integrity before hashing
            if (job.date === 'Unknown Date' || job.eventName === 'Unknown Event') continue;
            
            const id = createJobId(job.date, job.eventName, job.doctorName);
            
            if (await isJobNew(id)) {
                newJobs.push(job);
                await saveJobToHistory(id, job);
            }
        }

        // 3. Send Notification
        if (newJobs.length > 0) {
            const heading = ONLY_NOTIFY_VACANCIES 
                ? `🚨 *NEW DOCTOR VACANCIES FOUND* (${newJobs.length})`
                : `🚨 *NEW DOCTOR JOBS POSTED* (${newJobs.length})`;

            const list = newJobs.map((j, i) => 
                `${i + 1}. *${j.eventName}* on ${j.date}\n- Status: ${j.doctorName.includes('Unassigned') ? '**VACANT**' : j.doctorName}`
            ).join('\n\n');
            
            await sendTelegram(`${heading}\n\n${list}\n\n[View Jobs](${JOBS_PAGE_URL})`);
            logger.info(`[NOTIFY] Sent Telegram alert for ${newJobs.length} new jobs.`);
        } else {
            logger.info('[SCRAPE] No new unique jobs found after filtering.');
        }

        return { status: "success", new: newJobs.length };

    } catch (e) {
        // Critical failures
        logger.error(`[CRITICAL] ${e.message}`);
        await sendTelegram(`❌ CRITICAL SCRAPER FAILURE\n\nError: \`${e.message}\`\n\n*Check logs and GitHub Secrets.*`);
        return { status: "error", message: e.message };
    } finally {
        if (browser) await browser.close();
    }
}

// --- 6. EXECUTION BLOCK (NO SERVER) ---
(async () => {
    logger.info('[START] Starting Scheduled Scrape...');
    const result = await mainScraper();
    
    if (result.status === 'success') {
        logger.info(`[EXIT] Scrape completed successfully. New jobs found: ${result.new}`);
        process.exit(0);
    } else {
        logger.error('[EXIT] Scrape failed.');
        process.exit(1);
    }
})();
