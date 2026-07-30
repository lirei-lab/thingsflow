#!/usr/bin/env node
const fs = require('fs');

const baseUrl = process.env.BRIDGE_URL || 'http://localhost:8082';
const bridgeConnectUrl = process.env.BRIDGE_CONNECT_URL || baseUrl;
const bridgeHostHeader = process.env.BRIDGE_HOST_HEADER || '';
const uiUrl = process.env.TB_UI_URL || 'http://localhost:3001';
const hostResolverRules = process.env.TB_UI_HOST_RESOLVER_RULES || '';
const providedJwt = process.env.TB_UI_JWT || '';
const providedDashboardIds = process.env.TB_UI_DASHBOARD_IDS || '';
const chromiumExecutable = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE || [
  '/snap/bin/chromium',
  '/usr/bin/chromium',
  '/usr/bin/chromium-browser',
  '/usr/bin/google-chrome',
  '/usr/bin/google-chrome-stable'
].find((candidate) => fs.existsSync(candidate));
const dashboardTitles = (process.env.UI_SMOKE_DASHBOARDS || 'Thermostats,SCADA Process Demo,Smart Building Office Demo')
  .split(',')
  .map((title) => title.trim())
  .filter(Boolean);

function isUnexpectedApiFailure(response) {
  const url = response.url();
  if (!url.includes('/api/')) return false;
  const status = response.status();
  if (status < 400) return false;
  return true;
}

function parseProvidedDashboardIds(spec) {
  const byTitle = new Map();
  if (!spec) return byTitle;
  for (const entry of spec.split(',')) {
    const separator = entry.lastIndexOf('=');
    if (separator <= 0) continue;
    const title = entry.slice(0, separator).trim();
    const id = entry.slice(separator + 1).trim();
    if (title && id) {
      byTitle.set(title, { title, id: { id } });
    }
  }
  return byTitle;
}

async function main() {
  let chromium;
  try {
    ({ chromium } = require('playwright'));
  } catch (err) {
    console.error('Playwright is not installed. Run this smoke in an environment with the playwright package available.');
    process.exit(2);
  }

  let token = providedJwt;
  if (!token) {
    const loginHeaders = { 'content-type': 'application/json' };
    if (bridgeHostHeader) loginHeaders.host = bridgeHostHeader;
    const login = await fetch(`${bridgeConnectUrl}/api/auth/login`, {
      method: 'POST',
      headers: loginHeaders,
      body: JSON.stringify({ username: 'customer@thingsboard.org', password: 'customer' })
    });
    if (!login.ok) throw new Error(`login failed: HTTP ${login.status}`);
    ({ token } = await login.json());
  }

  const byTitle = parseProvidedDashboardIds(providedDashboardIds);
  if (!byTitle.size) {
    const dashboardHeaders = { 'X-Authorization': `Bearer ${token}` };
    if (bridgeHostHeader) dashboardHeaders.host = bridgeHostHeader;
    const dashboardsRes = await fetch(`${bridgeConnectUrl}/api/customer/dashboards?pageSize=20&page=0&sortProperty=title&sortOrder=ASC`, {
      headers: dashboardHeaders
    });
    if (!dashboardsRes.ok) throw new Error(`dashboard list failed: HTTP ${dashboardsRes.status}`);
    const dashboards = await dashboardsRes.json();
    for (const dashboard of dashboards.data || []) {
      byTitle.set(dashboard.title, dashboard);
    }
  }
  for (const title of dashboardTitles) {
    if (!byTitle.has(title)) throw new Error(`dashboard missing from API: ${title}`);
  }

  const launchOptions = {
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  };
  if (hostResolverRules) {
    launchOptions.args.push(`--host-resolver-rules=${hostResolverRules}`);
  }
  if (chromiumExecutable) {
    launchOptions.executablePath = chromiumExecutable;
  }
  const browser = await chromium.launch(launchOptions);
  const page = await browser.newPage({ viewport: { width: 1366, height: 900 } });
  const consoleErrors = [];
  const apiFailures = [];
  page.on('console', (msg) => {
    if (msg.type() === 'error') {
      consoleErrors.push(msg.text());
    }
  });
  page.on('pageerror', (err) => {
    consoleErrors.push(err.stack || err.message || String(err));
  });
  page.on('response', (response) => {
    if (isUnexpectedApiFailure(response)) {
      apiFailures.push(`${response.status()} ${response.request().method()} ${response.url()}`);
    }
  });

  await page.addInitScript((jwt) => {
    window.localStorage.setItem('jwt_token', jwt);
  }, token);

  for (const title of dashboardTitles) {
    const dashboard = byTitle.get(title);
    if (!dashboard) throw new Error(`dashboard missing from API: ${title}`);
    await page.goto(`${uiUrl}/dashboards/${dashboard.id.id}`, { waitUntil: 'networkidle', timeout: 45000 });
    await page.waitForTimeout(2500);
    const result = await page.evaluate(() => {
      const bodyText = document.body.innerText || '';
      const widgets = Array.from(document.querySelectorAll('tb-widget, .tb-widget, mat-card, table, canvas'));
      const visibleWidgets = widgets.filter((node) => {
        const rect = node.getBoundingClientRect();
        const style = window.getComputedStyle(node);
        return rect.width > 20 && rect.height > 20 && style.visibility !== 'hidden' && style.display !== 'none';
      });
      return {
        bodyLength: bodyText.trim().length,
        visibleWidgetCount: visibleWidgets.length,
        hasTitle: bodyText.includes(document.querySelector('h1,h2')?.textContent || '')
      };
    });
    if (result.visibleWidgetCount < 1 || result.bodyLength < 40) {
      throw new Error(`${title} rendered blank: ${JSON.stringify(result)}`);
    }
    console.log(`visual ok: ${title}`);
  }

  if (apiFailures.length) {
    throw new Error(`unexpected API failures:\n${[...new Set(apiFailures)].join('\n')}`);
  }
  if (consoleErrors.length) {
    throw new Error(`browser console/page errors:\n${[...new Set(consoleErrors)].slice(0, 20).join('\n')}`);
  }

  await browser.close();
}

main().catch((err) => {
  console.error(err.stack || err.message || err);
  process.exit(1);
});
