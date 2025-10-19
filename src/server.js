require('dotenv').config();
const express = require('express');
const cors = require('cors');
const net = require('net');
const path = require('path');
const { FileLogger } = require('./utils/file-logger');

// Initialize file logger
const LOG_TO_CONSOLE = /^true$/i.test(process.env.LOG_TO_CONSOLE || 'true');
const logger = new FileLogger({
  dir: process.env.LOG_DIR || path.join(process.cwd(), 'logs'),
  prefix: 'server',
  console: LOG_TO_CONSOLE,
});

const app = express();
app.use(cors());
app.use(express.json());

// Request log middleware (lightweight)
app.use((req, res, next) => {
  res.on('finish', () => {
    logger.info(`${req.method} ${req.originalUrl} ${res.statusCode}`);
  });
  next();
});

// Serve static UI
app.use(express.static(path.join(__dirname, '../public')));

const MINER_HOST = process.env.MINER_HOST || '192.168.0.243';
const MINER_PORT = parseInt(process.env.MINER_PORT || '4028', 10);
const PAPI_SEND_NULL = /^true$/i.test(process.env.PAPI_SEND_NULL || 'false');
const PAPI_APPEND_NEWLINE = /^true$/i.test(process.env.PAPI_APPEND_NEWLINE || 'false');

function splitTopLevelJSONObjects(s) {
  const parts = [];
  let depth = 0;
  let start = -1;
  let inStr = false;
  let esc = false;
  for (let i = 0; i < s.length; i += 1) {
    const ch = s[i];
    if (inStr) {
      if (esc) {
        esc = false;
        continue;
      }
      if (ch === '\\') {
        esc = true;
        continue;
      }
      if (ch === '"') {
        inStr = false;
      }
      continue;
    }
    if (ch === '"') {
      inStr = true;
      continue;
    }
    if (ch === '{') {
      if (depth === 0) start = i;
      depth++;
    } else if (ch === '}') {
      depth--;
      if (depth === 0 && start !== -1) {
        parts.push(s.slice(start, i + 1));
        start = -1;
      }
    }
  }
  return parts;
}

function cleanResponse(raw) {
  if (!raw) return '';
  // Remove NUL and other control chars except tab (\t), CR (\r), LF (\n)
  return raw.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, '').trim();
}

function papiRequest(command) {
  return new Promise((resolve, reject) => {
    const client = new net.Socket();
    let data = '';
    let settled = false;

    const settle = (err, val) => {
      if (settled) return;
      settled = true;
      err ? reject(err) : resolve(val);
    };

    client.setEncoding('utf8');
    client.setTimeout(8000);

    client.on('timeout', () => {
      client.destroy(new Error('Socket timeout'));
    });
    client.on('error', (err) => {
      logger.error('papi socket error', { message: err.message });
      settle(err);
    });

    client.connect(MINER_PORT, MINER_HOST, () => {
      let payload = JSON.stringify({ command });
      if (PAPI_APPEND_NEWLINE) payload += '\n';
      if (PAPI_SEND_NULL) payload += '\u0000';
      client.write(payload);
      client.end(); // half-close to signal end of request
    });

    client.on('data', (chunk) => {
      data += chunk;
    });

    const finalize = () => {
      const cleaned = cleanResponse(data);
      if (!cleaned) return settle(new Error('Empty response from miner'));
      try {
        return settle(null, JSON.parse(cleaned));
      } catch (e) {
        const parts = splitTopLevelJSONObjects(cleaned);
        if (parts.length > 1) return settle(null, { parts });
        // Surface a short preview for diagnostics in logs
        logger.warn('[papi] Invalid JSON preview', { preview: cleaned.slice(0, 200) });
        return settle(new Error('Invalid JSON from miner'));
      }
    };

    client.on('end', finalize);
    client.on('close', finalize);
  });
}

// Existing estats-based extraction
function extractPower(estatsRoot) {
  const totals = [];
  const perChain = [];

  const isNum = (v) => typeof v === 'number' && Number.isFinite(v);
  const plausibleTotal = (v) => isNum(v) && v >= 50 && v <= 10000; // W
  const plausibleChain = (v) => isNum(v) && v >= 5 && v <= 5000; // W

  const totalKeyRe = /^(power(\s*(avg|now|total)?)|wattage|total\s*w(at|att)s?)$/i;
  const chainKeyRe = /(chain|board).*?(power|watt)s?/i;
  const genericPowerRe = /(power|watt)s?/i;

  const visit = (node) => {
    if (!node) return;
    if (Array.isArray(node)) {
      for (const v of node) visit(v);
      return;
    }
    if (typeof node !== 'object') return;

    for (const [k, v] of Object.entries(node)) {
      if (isNum(v)) {
        const key = String(k);
        if (totalKeyRe.test(key) && plausibleTotal(v)) totals.push(v);
        else if (chainKeyRe.test(key) && plausibleChain(v)) perChain.push(v);
        else if (genericPowerRe.test(key) && plausibleChain(v)) perChain.push(v);
      } else if (v && typeof v === 'object') {
        visit(v);
      }
    }
  };

  visit(estatsRoot);

  // Prefer explicit total if present; else sum per-chain (dedup simple repeats)
  let total = null;
  if (totals.length) total = Math.max(...totals);
  if (total == null && perChain.length) total = perChain.reduce((a, b) => a + b, 0);

  // Round sensibly
  if (total != null) total = Math.round(total);
  const perChainRounded = perChain.slice(0, 16).map((n) => Math.round(n));
  return { total, per_chain: perChainRounded };
}

// New: extract from tunerstatus
function extractPowerFromTuner(tsRoot) {
  try {
    const ts = tsRoot?.TUNERSTATUS?.[0];
    if (!ts) return null;
    const total = Number(ts.ApproximateMinerPowerConsumption);
    const per_chain = Array.isArray(ts.TunerChainStatus)
      ? ts.TunerChainStatus.map((c) =>
          Math.round(Number(c.ApproximatePowerConsumptionWatt) || 0)
        ).filter((n) => n > 0)
      : [];
    const result = {};
    if (Number.isFinite(total)) result.total = Math.round(total);
    if (per_chain.length) result.per_chain = per_chain;
    return result.total != null || (result.per_chain && result.per_chain.length) ? result : null;
  } catch (_) {
    return null;
  }
}

app.get('/api/version', async (req, res) => {
  try {
    const resp = await papiRequest('version');
    res.json(resp);
  } catch (e) {
    logger.error('/api/version failed', { message: e.message });
    res.status(502).json({ error: e.message });
  }
});

app.get('/api/raw', async (req, res) => {
  const cmd = req.query.cmd || 'summary+devs+pools';
  try {
    // Do a low-level call and return the cleaned string for debugging
    const client = new net.Socket();
    let data = '';
    await new Promise((resolve, reject) => {
      client.setEncoding('utf8');
      client.setTimeout(8000);
      client.on('timeout', () => client.destroy(new Error('Socket timeout')));
      client.on('error', reject);
      client.connect(MINER_PORT, MINER_HOST, () => {
        let payload = JSON.stringify({ command: cmd });
        if (PAPI_APPEND_NEWLINE) payload += '\n';
        if (PAPI_SEND_NULL) payload += '\u0000';
        client.write(payload);
        client.end();
      });
      client.on('data', (chunk) => {
        data += chunk;
      });
      const done = () => resolve();
      client.on('end', done);
      client.on('close', done);
    });
    const cleaned = cleanResponse(data);
    res.json({
      length: cleaned.length,
      preview: cleaned.slice(0, 500),
      full: cleaned.slice(0, 5000),
    });
  } catch (e) {
    logger.error('/api/raw failed', { message: e.message });
    res.status(502).json({ error: e.message });
  }
});

app.get('/api/overview', async (req, res) => {
  try {
    // Try combined first (works well via netcat); fallback to separate
    let combined = await papiRequest('summary+devs+pools');
    if (combined && combined.parts) {
      // If concatenated, try to parse first part
      combined = JSON.parse(combined.parts[0]);
    }
    if (combined && combined.summary && combined.devs && combined.pools) {
      return res.json(combined);
    }

    const [summary, devs, pools] = await Promise.all([
      papiRequest('summary'),
      papiRequest('devs'),
      papiRequest('pools'),
    ]);
    const pick = (v) => (v && v.parts ? JSON.parse(v.parts[0]) : v);
    const out = { summary: [pick(summary)], devs: [pick(devs)], pools: [pick(pools)], id: 1 };
    res.json(out);
  } catch (e) {
    logger.error('/api/overview failed', { message: e.message });
    res.status(502).json({ error: e.message });
  }
});

app.get('/api/estats', async (req, res) => {
  try {
    const resp = await papiRequest('estats');
    if (resp && resp.parts) return res.json(JSON.parse(resp.parts[0]));
    res.json(resp);
  } catch (e) {
    logger.error('/api/estats failed', { message: e.message });
    res.status(502).json({ error: e.message });
  }
});

// Updated: Power endpoint prefers tunerstatus, falls back to estats
app.get('/api/power', async (req, res) => {
  try {
    let tuner = await papiRequest('tunerstatus');
    if (tuner && tuner.parts) tuner = JSON.parse(tuner.parts[0]);
    let power = extractPowerFromTuner(tuner);

    if (!power) {
      let estats = await papiRequest('estats');
      if (estats && estats.parts) estats = JSON.parse(estats.parts[0]);
      power = extractPower(estats);
    }

    res.json(power || { total: null, per_chain: [] });
  } catch (e) {
    logger.error('/api/power failed', { message: e.message });
    res.status(502).json({ error: e.message });
  }
});

// Fallback to index.html for root
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, '../public/index.html'));
});

const FRONTEND_PORT = process.env.FRONTEND_PORT || 3030;
app.listen(FRONTEND_PORT, () => {
  logger.info(`Server running on http://localhost:${FRONTEND_PORT}`);
});
module.exports = { app };
