// Usage: node scrape_website.js <url>
// Fetches fully-rendered HTML from a URL using Puppeteer and prints to stdout.

const puppeteer = require('puppeteer');

const url = process.argv[2];

if (!url) {
  console.error('Usage: node scrape_website.js <url>');
  process.exit(1);
}

(async () => {
  let browser;
  try {
    browser = await puppeteer.launch({ headless: true });
    const page = await browser.newPage();
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 10000 });
    const html = await page.content();
    console.log(html);
  } catch (e) {
    console.error('Puppeteer error: ' + e.message);
    process.exit(1);
  } finally {
    if (browser) await browser.close();
  }
})();
