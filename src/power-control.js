const express = require('express');
const cors = require('cors');
const { exec } = require('child_process');
const mqtt = require('mqtt');
const http = require('http');
const { Server } = require('socket.io');
const net = require('net');
const { FileLogger } = require('./utils/file-logger');
require('dotenv').config();
const SSH_OPTS = process.env.SSH_OPTS || '-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null';

// Initialize file logger
const LOG_TO_CONSOLE = /^true$/i.test(process.env.LOG_TO_CONSOLE || 'true');
const logger = new FileLogger({ 
  dir: process.env.LOG_DIR || require('path').join(process.cwd(), 'logs'), 
  prefix: 'power-control', 
  console: LOG_TO_CONSOLE 
});

const app = express();
const corsOptions = {
  origin: (origin, cb) => cb(null, true), // reflect origin for dev
  methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  credentials: true,
};
app.use(cors(corsOptions));
app.use(express.json());
// Serve static frontend
app.use(express.static(require('path').join(__dirname, '..', 'public')));

const MINER_HOSTS = (process.env.MINER_HOSTS || '').split(',').map(ip => ip.trim()).filter(Boolean);
const MINER_PORT = parseInt(process.env.MINER_PORT || '4028', 10);

function getMinerHost(req) {
  // Accept ?miner=0 or ?ip=192.168.x.x
  const idx = req.query.miner ? parseInt(req.query.miner, 10) : null;
  if (idx != null && MINER_HOSTS[idx]) return MINER_HOSTS[idx];
  if (req.query.ip && MINER_HOSTS.includes(req.query.ip)) return req.query.ip;
  // Default to first miner
  return MINER_HOSTS[0];
}

const VENUS_ID = 'c0619ab8cc67';

logger.info('Connecting to MQTT broker...');
const mqttClient = mqtt.connect({
  host: '192.168.0.50',
  port: 1883,
  protocol: 'mqtt',
  reconnectPeriod: 5000, // Auto-reconnect
});

let keepAliveInterval = null; // Track interval for proper cleanup

let latestData = {
  power: { l1: 0, l2: 0, l3: 0 },
  consumption: { l1: 0, l2: 0, l3: 0 },
  battery: { soc: null, voltage: null, current: null, power: null }
};

const topics = [
  'N/+/grid/+/Ac/L1/Power',
  'N/+/grid/+/Ac/L2/Power',
  'N/+/grid/+/Ac/L3/Power',
  'N/+/system/+/Ac/ConsumptionOnInput/L1/Power',
  'N/+/system/+/Ac/ConsumptionOnInput/L2/Power',
  'N/+/system/+/Ac/ConsumptionOnInput/L3/Power',
  'N/+/battery/+/Soc',
  'N/+/battery/+/Dc/0/Power',
  'N/+/battery/+/Dc/0/Current',
  'N/+/battery/+/Dc/0/Voltage'
];

const PAPI_APPEND_NEWLINE = /^true$/i.test(process.env.PAPI_APPEND_NEWLINE || 'false');
const PAPI_SEND_NULL = /^true$/i.test(process.env.PAPI_SEND_NULL || 'false');

// Auto-shutdown config (minutes). 0 disables.
const AUTO_SHUTDOWN_MINUTES = parseInt(process.env.AUTO_SHUTDOWN_MINUTES || '0', 10);
// Auto-resume config (minutes of sustained surplus to start miners). 0 disables.
const AUTO_RESUME_MINUTES = parseInt(process.env.AUTO_RESUME_MINUTES || '3', 10);

// Replace noisy per-request logging with optional logging controlled by env
const REQUEST_LOG = /^true$/i.test(process.env.REQUEST_LOG || '');
const REQUEST_LOG_IGNORE_PREFIX = (process.env.REQUEST_LOG_IGNORE_PREFIX || '/api/miners').split(',').map(s => s.trim()).filter(Boolean);

mqttClient.on('connect', () => {
  logger.info('Connected to MQTT broker');
  mqttClient.subscribe(topics, (err) => {
    if (!err) {
      logger.info(`Subscribed to topics: ${topics.join(', ')}`);
    } else {
      logger.error('Subscription error', { error: err.message });
      return; // Don't end client, let it auto-reconnect
    }
  });
  
  // Clear any existing interval and create new one
  if (keepAliveInterval) clearInterval(keepAliveInterval);
  keepAliveInterval = setInterval(() => {
    if (mqttClient.connected) {
      mqttClient.publish(`R/${VENUS_ID}/keepalive` , '');
    }
  }, 30000);
});

mqttClient.on('message', (topic, message) => {
  try {
    const data = JSON.parse(message.toString());
    const value = parseFloat(data.value);
    const topicParts = topic.split('/');
    const category = topicParts[2];
    const metric = topicParts.slice(4).join('/');
    
    if (category === 'grid') {
      if (metric.startsWith('Ac/L1/Power')) latestData.power.l1 = value;
      else if (metric.startsWith('Ac/L2/Power')) latestData.power.l2 = value;
      else if (metric.startsWith('Ac/L3/Power')) latestData.power.l3 = value;
    } else if (category === 'system' && metric.includes('ConsumptionOnInput')) {
      if (metric.endsWith('L1/Power')) latestData.consumption.l1 = value;
      else if (metric.endsWith('L2/Power')) latestData.consumption.l2 = value;
      else if (metric.endsWith('L3/Power')) latestData.consumption.l3 = value;
    } else if (category === 'battery') {
      if (metric === 'Soc') {
        latestData.battery.soc = value;
      } else if (metric === 'Dc/0/Power') {
        latestData.battery.power = value;
      } else if (metric === 'Dc/0/Current') {
        latestData.battery.current = value;
      } else if (metric === 'Dc/0/Voltage') {
        latestData.battery.voltage = value;
      }
    }
  } catch (e) {
    logger.warn('Could not parse MQTT message', { topic, message: message.toString(), error: e.message });
  }
});

// Create server and socket.io first
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*', // Allow all origins
    methods: ['GET', 'POST'],
    credentials: false // Credentials not needed for wildcard
  }
});

// Send data to clients every 5 seconds
setInterval(() => {
  const totalPower = latestData.power.l1 + latestData.power.l2 + latestData.power.l3;
  const totalConsumption = latestData.consumption.l1 + latestData.consumption.l2 + latestData.consumption.l3;
  const dataToSend = {
    power: { ...latestData.power, total: totalPower },
    consumption: { ...latestData.consumption, total: totalConsumption },
    battery: { ...latestData.battery },
    timestamp: new Date().toISOString()
  };
  
  io.emit('data_update', dataToSend);
  latestData.lastSent = dataToSend;
}, 5000);

mqttClient.on('error', (err) => {
  logger.error('MQTT connection error', { error: err.message });
  // Don't call mqttClient.end() - let it auto-reconnect
});

mqttClient.on('close', () => {
  logger.info('Connection to MQTT broker closed');
  if (keepAliveInterval) {
    clearInterval(keepAliveInterval);
    keepAliveInterval = null;
  }
});

mqttClient.on('reconnect', () => {
  logger.info('Reconnecting to MQTT broker...');
});

// Optional request logging (disabled by default). To enable: REQUEST_LOG=true
app.use((req, res, next) => {
  if (REQUEST_LOG && !REQUEST_LOG_IGNORE_PREFIX.some(p => p && req.url.startsWith(p))) {
    logger.info(`${req.method} ${req.url}`, { origin: req.headers.origin || 'unknown' });
  }
  next();
});

app.get('/api/venus', async (req, res) => {
  // Forward to the correct port if needed
  try {
    // If running on the same process, serve directly
    res.json(latestData.lastSent || {});
  } catch (e) {
    res.status(502).json({ error: e.message });
  }
});

app.get('/api/overview', async (req, res) => {
  const minerHost = getMinerHost(req);
  try {
    // Try combined first (works well via netcat); fallback to separate
    let combined = await papiRequest('summary+devs+pools', minerHost);
    
    let summary, devs, pools;
    
    if (combined && combined.parts) {
      // Multiple JSON objects returned as parts
      const parsed = combined.parts.map(part => JSON.parse(part));
      summary = parsed.find(p => p.summary)?.[0] || {};
      devs = parsed.find(p => p.devs) || [];
      pools = parsed.find(p => p.pools) || [];
    } else if (combined && combined.summary && combined.devs && combined.pools) {
      // Single combined response
      summary = combined.summary[0] || {};
      devs = combined.devs || [];
      pools = combined.pools || [];
    } else {
      // Fallback to separate requests
      const [summaryResp, devsResp, poolsResp] = await Promise.all([
        papiRequest('summary', minerHost),
        papiRequest('devs', minerHost),
        papiRequest('pools', minerHost),
      ]);
      
      const pick = (v) => (v && v.parts ? JSON.parse(v.parts[0]) : v);
      summary = pick(summaryResp)?.summary?.[0] || {};
      devs = pick(devsResp)?.devs || [];
      pools = pick(poolsResp)?.pools || [];
    }

    res.json({ summary: [{ summary: [summary] }], devs: [{ devs }], pools: [{ pools }] });
  } catch (e) {
    res.status(502).json({ error: e.message });
  }
});

// Power extraction functions
function extractPower(estatsRoot) {
  const totals = [];
  const perChain = [];

  const isNum = (v) => typeof v === 'number' && isFinite(v);
  const plausibleTotal = (v) => isNum(v) && v >= 50 && v <= 10000; // W
  const plausibleChain = (v) => isNum(v) && v >= 5 && v <= 5000; // W

  const totalKeyRe = /^(power(\s*(avg|now|total)?)|wattage|total\s*w(at|att)s?)$/i;
  const chainKeyRe = /(chain|board).*?(power|watt)s?/i;

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
      } else if (v && typeof v === 'object') {
        visit(v);
      }
    }
  };

  visit(estatsRoot);

  let total = null;
  if (totals.length) total = Math.max(...totals);
  if (total == null && perChain.length) total = perChain.reduce((a, b) => a + b, 0);

  if (total != null) total = Math.round(total);
  const per_chain = perChain.slice(0, 16).map((n) => Math.round(n));
  return { total, per_chain };
}

function extractPowerFromTuner(tsRoot) {
  try {
    const ts = tsRoot?.TUNERSTATUS?.[0];
    if (!ts) return null;
    const total = Number(ts.ApproximateMinerPowerConsumption);
    const per_chain = Array.isArray(ts.TunerChainStatus)
      ? ts.TunerChainStatus.map((c) => Math.round(Number(c.ApproximatePowerConsumptionWatt) || 0)).filter((n) => n > 0)
      : [];
    const result = {};
    if (Number.isFinite(total)) result.total = Math.round(total);
    if (per_chain.length) result.per_chain = per_chain;
    return (result.total != null || (result.per_chain && result.per_chain.length)) ? result : null;
  } catch (_) { return null; }
}

// Helper to get miner power for a given host
async function getMinerPower(host) {
  try {
    let tuner = await papiRequest('tunerstatus', host);
    if (tuner && tuner.parts) tuner = JSON.parse(tuner.parts[0]);
    let power = extractPowerFromTuner(tuner);

    if (!power) {
      let estats = await papiRequest('estats', host);
      if (estats && estats.parts) estats = JSON.parse(estats.parts[0]);
      power = extractPower(estats);
    }

    return power || { total: null, per_chain: [] };
  } catch (e) {
    return { total: null, per_chain: [], error: e.message };
  }
}

app.get('/api/power', async (req, res) => {
  const minerHost = getMinerHost(req);
  try {
    let tuner = await papiRequest('tunerstatus', minerHost);
    if (tuner && tuner.parts) tuner = JSON.parse(tuner.parts[0]);
    let power = extractPowerFromTuner(tuner);

    if (!power) {
      let estats = await papiRequest('estats', minerHost);
      if (estats && estats.parts) estats = JSON.parse(estats.parts[0]);
      power = extractPower(estats);
    }

    res.json(power || { total: null, per_chain: [] });
  } catch (e) {
    res.status(502).json({ error: e.message });
  }
});

app.post('/api/power-target', async (req, res) => {
  const minerHost = getMinerHost(req);
  console.log('Raw body:', req.body);
  if (req.body.shutdown) {
    // SSH command to completely stop BOSminer
    const sshCmd = `ssh ${SSH_OPTS} root@${minerHost} "/etc/init.d/bosminer stop"`;
    exec(sshCmd, (err, stdout, stderr) => {
      if (err) return res.status(500).json({ error: stderr || err.message });
      res.json({ ok: true, shutdown: true });
    });
    return;
  }
  if (req.body.resume) {
    // SSH command to start BOSminer
    const sshCmd = `ssh ${SSH_OPTS} root@${minerHost} "/etc/init.d/bosminer start"`;
    exec(sshCmd, (err, stdout, stderr) => {
      if (err) return res.status(500).json({ error: stderr || err.message });
      res.json({ ok: true, resume: true });
    });
    return;
  }
  console.log('Received watts:', req.body.watts, typeof req.body.watts);
  const watts = Number(req.body.watts);
  if (isNaN(watts) || watts < 0 || watts > 1600) return res.status(400).json({ error: 'Invalid value' });

  // SSH command to update power_target and reload bosminer
  const sshCmd = `ssh ${SSH_OPTS} root@${minerHost} "sed -i -E 's/^([[:space:]]*power_target[[:space:]]*=[[:space:]]*)[0-9]+/\\1 ${watts}/' /etc/bosminer.toml && /etc/init.d/bosminer reload"`;

  exec(sshCmd, (err, stdout, stderr) => {
    if (err) return res.status(500).json({ error: stderr || err.message });
    res.json({ ok: true, watts });
  });
});

// --- Solar surplus miner control logic ---
const SURPLUS_CHECK_INTERVAL = 30000; // ms
const MOVING_AVG_WINDOW = 10; // window length for moving avg of grid power samples
const MIN_POWER = 0; // W per miner when idling/paused
const MAX_POWER = 1420; // W per miner maximum target
const GRID_SURPLUS_THRESHOLD = -100; // W (negative = exporting to grid => surplus)
const GRID_DEFICIT_THRESHOLD = 100; // W (positive = importing from grid)
const HYSTERESIS = 50; // W noise band

// Discrete two-miner combo control
const DISCRETE_MODE = true; // enable combo-based control for 2 miners
const ALLOWED_LEVELS = [0, 800, 1000, 1420];

let gridPowerHistory = [];
let lastAutoSet = 0;
let autoControlEnabled = true; // can be toggled via UI later

// Track last applied targets per miner for smooth control and for fair distribution
const lastAutoTargets = Object.create(null); // host -> watts (logical level, 0 means OFF)
const lastApplyTs = Object.create(null); // host -> timestamp
let rrIndex = 0; // round-robin start index for fair surplus distribution
const minerStopped = Object.create(null); // host -> boolean

// Auto-shutdown state
let noSurplusStartTs = null; // timestamp when "no surplus" period started
let minersAreShutDown = false; // set to true after we issue shutdown to all miners
// Auto-resume state
let surplusHoldStartTs = null; // when sustained surplus started while miners are off

function getMovingAvg(arr) {
  if (!arr.length) return 0;
  return arr.reduce((a,b)=>a+b,0)/arr.length;
}

function clamp(v, lo, hi){ return Math.max(lo, Math.min(hi, v)); }

function reorderHostsFair(hosts){
  if (!hosts.length) return hosts;
  rrIndex = (rrIndex + 1) % hosts.length;
  return hosts.slice(rrIndex).concat(hosts.slice(0, rrIndex));
}

function applyMinerPower(host, logicalTarget){
  return new Promise((resolve) => {
    const now = Date.now();
    const prev = Number.isFinite(lastAutoTargets[host]) ? lastAutoTargets[host] : 0;

    // Skip if no change
    if (prev === logicalTarget) return resolve({ host, skipped: true, reason: 'no-change' });

    // Per-miner OFF (full BOSminer stop)
    if (logicalTarget === 0) {
      if (minerStopped[host]) return resolve({ host, skipped: true, reason: 'already-off' });
      const sshCmd = `ssh ${SSH_OPTS} root@${host} "/etc/init.d/bosminer stop"`;
      exec(sshCmd, (err, stdout, stderr) => {
        if (err) {
          logger.error(`Stop error on ${host}`, { error: stderr || err.message });
          return resolve({ host, error: stderr || err.message });
        }
        minerStopped[host] = true;
        lastAutoTargets[host] = 0;
        lastApplyTs[host] = now;
        logger.info(`Set ${host} -> OFF (was ${prev}W)`);
        resolve({ host, applied: 0 });
      });
      return;
    }

    // Turning ON or changing power
    const parts = [];
    if (minerStopped[host]) {
      parts.push('/etc/init.d/bosminer start');
      minerStopped[host] = false;
    }
    parts.push(`sed -i -E 's/^([[:space:]]*power_target[[:space:]]*=[[:space:]]*)[0-9]+/\\1 ${logicalTarget}/' /etc/bosminer.toml`);
    parts.push('/etc/init.d/bosminer reload');
    const sshCmd = `ssh ${SSH_OPTS} root@${host} "${parts.join(' && ')}"`;

    exec(sshCmd, (err, stdout, stderr) => {
      if (err) {
        logger.error(`SSH error on ${host}`, { error: stderr || err.message });
        return resolve({ host, error: stderr || err.message });
      }
      lastAutoTargets[host] = logicalTarget;
      lastApplyTs[host] = now;
      logger.info(`Set ${host} -> ${logicalTarget}W (was ${prev}W${prev===0?' OFF':''})`);
      resolve({ host, applied: logicalTarget });
    });
  });
}

// Helper to stop all miners (BOSminer stop)
async function shutdownAllMiners(){
  const tasks = MINER_HOSTS.map(host => new Promise((resolve) => {
    const sshCmd = `ssh ${SSH_OPTS} root@${host} "/etc/init.d/bosminer stop"`;
    exec(sshCmd, (err) => resolve({ host, ok: !err }));
  }));
  return Promise.allSettled(tasks);
}

// Helper to start all miners (BOSminer start)
async function startAllMiners(){
  const tasks = MINER_HOSTS.map(host => new Promise((resolve) => {
    const sshCmd = `ssh ${SSH_OPTS} root@${host} "/etc/init.d/bosminer start"`;
    exec(sshCmd, (err) => resolve({ host, ok: !err }));
  }));
  return Promise.allSettled(tasks);
}

async function autoSetMinerPower() {
  if (!autoControlEnabled) return;
  try {
    const gridPower = latestData.power.l1 + latestData.power.l2 + latestData.power.l3; // positive = importing, negative = exporting
    gridPowerHistory.push(gridPower);
    if (gridPowerHistory.length > MOVING_AVG_WINDOW) gridPowerHistory.shift();
    const avgGridPower = getMovingAvg(gridPowerHistory);
    const now = Date.now();
    // Only act every interval
    if (now - lastAutoSet < SURPLUS_CHECK_INTERVAL) return;
    lastAutoSet = now;

    if (!MINER_HOSTS.length) {
      logger.warn('No miners configured in MINER_HOSTS');
      return;
    }

    // Ensure we have initial targets
    for (const host of MINER_HOSTS) if (!Number.isFinite(lastAutoTargets[host])) lastAutoTargets[host] = MIN_POWER;

    // --- Measure actual miner power (prefer measured values when available) ---
    let measuredResults = [];
    try {
      measuredResults = await Promise.all(
        MINER_HOSTS.map(h => getMinerPower(h).catch(err => ({ total: null, error: err && err.message })))
      );
    } catch (e) {
      // should not happen because individual promises catch; still log
      logger.warn('Error measuring miners', { error: e.message });
    }
    const measuredMap = Object.fromEntries(MINER_HOSTS.map((h, i) => [h, Number.isFinite(measuredResults[i]?.total) ? measuredResults[i].total : null]));
    const measuredSum = MINER_HOSTS.reduce((s, h) => s + (Number.isFinite(measuredMap[h]) ? measuredMap[h] : 0), 0);

    logger.debug('Measured miner powers', { measuredMap, measuredSum, avgGridPower: avgGridPower.toFixed(1) });

    // --- Auto shutdown / resume (unchanged behavior, but use measured values for diagnostics) ---
    const shutdownEnabled = AUTO_SHUTDOWN_MINUTES > 0;
    const resumeEnabled = AUTO_RESUME_MINUTES > 0;

    if (minersAreShutDown) {
      if (resumeEnabled) {
        const resumeCond = gridPower < (GRID_SURPLUS_THRESHOLD - HYSTERESIS);
        const surplusAmount = Math.max(0, -gridPower);
        const thresholdWithHysteresis = GRID_SURPLUS_THRESHOLD - HYSTERESIS;

        logger.debug(`Resume evaluation: gridPower=${gridPower.toFixed(1)}W, avgGridPower=${avgGridPower.toFixed(1)}W, measuredSum=${measuredSum.toFixed(1)}W`, {
          instantGridPower: gridPower.toFixed(1),
          movingAvgGridPower: avgGridPower.toFixed(1),
          surplusDetected: surplusAmount.toFixed(1),
          resumeThreshold: thresholdWithHysteresis,
          conditionMet: resumeCond,
          requiredMinutes: AUTO_RESUME_MINUTES,
          currentlyHolding: !!surplusHoldStartTs
        });

        if (resumeCond) {
          if (!surplusHoldStartTs) {
            surplusHoldStartTs = now;
            logger.info(`Resume arming: sustained surplus detected (${surplusAmount.toFixed(1)}W export). Waiting ${AUTO_RESUME_MINUTES} min before resume...`);
          }
          const heldMs = now - (surplusHoldStartTs || now);
          if (heldMs >= AUTO_RESUME_MINUTES * 60000) {
            logger.info(`Auto-resume triggered: surplus held for ${heldMs/60000} minutes. Starting all miners.`);
            startAllMiners().then(() => {
              minersAreShutDown = false;
              surplusHoldStartTs = null;
              noSurplusStartTs = null;
              for (const h of MINER_HOSTS) { lastAutoTargets[h] = MIN_POWER; lastApplyTs[h] = 0; }
              io.emit('auto_control_update', { enabled: autoControlEnabled, autoEvent: 'resumed', lastAvgGridPower: avgGridPower });
              logger.info('Auto-resume completed: all miners started, ready for power targeting');
            });
          }
        } else {
          if (surplusHoldStartTs) surplusHoldStartTs = null;
        }
      } else {
        logger.debug('Resume disabled (AUTO_RESUME_MINUTES=0), miners will remain shut down');
      }
      return;
    }

    if (shutdownEnabled) {
      const noSurplus = gridPower >= 0;
      const importing = gridPower > (GRID_DEFICIT_THRESHOLD + HYSTERESIS);
      const shutdownCond = noSurplus || importing;
      if (shutdownCond) {
        if (!noSurplusStartTs) {
          noSurplusStartTs = now;
          logger.info(`Shutdown arming: no-surplus/deficit detected. Waiting ${AUTO_SHUTDOWN_MINUTES} min...`);
        }
        const heldMs = now - (noSurplusStartTs || now);
        if (heldMs >= AUTO_SHUTDOWN_MINUTES * 60000) {
          logger.info('Auto-shutdown: stopping all miners.');
          shutdownAllMiners().then(() => {
            minersAreShutDown = true;
            noSurplusStartTs = null;
            surplusHoldStartTs = null;
            io.emit('auto_control_update', { enabled: autoControlEnabled, autoEvent: 'shutdown', lastAvgGridPower: avgGridPower });
          });
          return;
        }
      } else if (noSurplusStartTs) {
        noSurplusStartTs = null;
      }
    }

    // Discrete N-miner combo control: pick N-levels from ALLOWED_LEVELS that best match the desired total
    if (DISCRETE_MODE) {
      const desiredTotal = Math.max(0, Math.round(-avgGridPower));
      logger.debug('Discrete mode decision', { desiredTotal, measuredSum, lastAutoTargets, miners: MINER_HOSTS.length });
      const nextCombo = pickBestCombo(desiredTotal, MINER_HOSTS.length);

      const actions = [];
      for (let i = 0; i < MINER_HOSTS.length; i++) {
        const host = MINER_HOSTS[i];
        const desired = nextCombo[i] || 0;
        actions.push(applyMinerPower(host, desired));
      }

      Promise.allSettled(actions).then(() => {
        io.emit('auto_control_update', {
          enabled: autoControlEnabled,
          lastAvgGridPower: avgGridPower,
          surplusW: Math.max(0, -avgGridPower),
          discreteCombo: nextCombo,
          perMinerTargets: Object.fromEntries(MINER_HOSTS.map(h => [h, lastAutoTargets[h] || 0]))
        });
      });
      return;
    }

    // Continuous control path (use measured values where available to compute remaining need)
    const hostsFair = reorderHostsFair(MINER_HOSTS);
    const actions = [];

    if (avgGridPower < GRID_SURPLUS_THRESHOLD - HYSTERESIS) {
      // Excess solar: increase miners to absorb surplus
      const desiredTotal = Math.round(-avgGridPower); // total W we'd like miners to consume

      // Compute current measured sum (fallback to lastAutoTargets if measurement missing)
      let currentSum = 0;
      for (const h of MINER_HOSTS) {
        if (Number.isFinite(measuredMap[h])) currentSum += measuredMap[h];
        else currentSum += Number.isFinite(lastAutoTargets[h]) ? lastAutoTargets[h] : 0;
      }

      let remaining = Math.max(0, desiredTotal - currentSum); // W to add

      // Compute capacity based on last applied targets
      let maxCapacity = 0;
      for (const host of hostsFair) {
        const cur = Number.isFinite(lastAutoTargets[host]) ? lastAutoTargets[host] : 0;
        maxCapacity += Math.max(0, MAX_POWER - cur);
      }
      if (remaining > maxCapacity) remaining = maxCapacity;

      logger.debug(`Surplus distribution: desiredTotal=${desiredTotal}W, measuredSum=${currentSum}W, remaining=${remaining}W, capacity=${maxCapacity}W`);

      for (const host of hostsFair) {
        if (remaining <= 0) break;
        const cur = Number.isFinite(lastAutoTargets[host]) ? lastAutoTargets[host] : 0;
        const headroom = Math.max(0, MAX_POWER - cur);
        const inc = Math.min(headroom, Math.ceil(remaining / Math.max(1, hostsFair.length)));
        const next = clamp(cur + inc, MIN_POWER, MAX_POWER);
        actions.push(applyMinerPower(host, next));
        remaining -= (next - cur);
      }
    } else if (avgGridPower > GRID_DEFICIT_THRESHOLD + HYSTERESIS) {
      // Importing from grid: reduce miners toward MIN_POWER
      let needReduce = Math.round(avgGridPower); // W to reduce
      const hostsByPower = hostsFair.slice().sort((a, b) => (lastAutoTargets[b] || MIN_POWER) - (lastAutoTargets[a] || MIN_POWER));

      let reducibleTotal = 0;
      for (const h of hostsByPower) reducibleTotal += Math.max(0, (lastAutoTargets[h] || MIN_POWER) - MIN_POWER);
      if (needReduce > reducibleTotal) needReduce = reducibleTotal;

      logger.debug(`Reducing miners: avgGrid=${avgGridPower.toFixed(1)}W, needReduce=${needReduce}W, reducibleTotal=${reducibleTotal}W`);

      for (const host of hostsByPower) {
        if (needReduce <= 0) break;
        const cur = lastAutoTargets[host];
        const reducible = Math.max(0, cur - MIN_POWER);
        const dec = Math.min(reducible, Math.ceil(needReduce / Math.max(1, hostsByPower.length)));
        const next = clamp(cur - dec, MIN_POWER, MAX_POWER);
        actions.push(applyMinerPower(host, next));
        needReduce -= (cur - next);
      }
    } else {
      logger.debug(`Within hysteresis. Avg grid: ${avgGridPower.toFixed(1)}W. No changes.`);
    }

    if (actions.length) {
      Promise.allSettled(actions).then((results) => {
        io.emit('auto_control_update', {
          enabled: autoControlEnabled,
          lastAvgGridPower: avgGridPower,
          perMinerTargets: Object.fromEntries(MINER_HOSTS.map(h => [h, lastAutoTargets[h]]))
        });
        logger.debug('Applied auto-control changes', { perMinerTargets: Object.fromEntries(MINER_HOSTS.map(h => [h, lastAutoTargets[h]])) });
      });
    } else {
      io.emit('auto_control_update', {
        enabled: autoControlEnabled,
        lastAvgGridPower: avgGridPower,
        perMinerTargets: Object.fromEntries(MINER_HOSTS.map(h => [h, lastAutoTargets[h]]))
      });
    }
  } catch (e) {
    logger.error('autoSetMinerPower error', { error: e && e.message });
  }
}
setInterval(autoSetMinerPower, SURPLUS_CHECK_INTERVAL);

const PORT = process.env.PORT || 3099;
server.listen(PORT, () => {
  logger.info(`Server is running on http://localhost:${PORT}`);
});

process.on('SIGINT', () => {
  logger.info('Disconnecting from MQTT broker...');
  if (keepAliveInterval) {
    clearInterval(keepAliveInterval);
    keepAliveInterval = null;
  }
  mqttClient.end();
  logger.close();
  process.exit();
});

app.post('/api/auto-control', (req, res) => {
  if (typeof req.body.enable === 'boolean') {
    autoControlEnabled = req.body.enable;

    // Log enable/disable with config and current status
    try {
      const cfg = `interval=${SURPLUS_CHECK_INTERVAL/1000}s, window=${MOVING_AVG_WINDOW}, shutdown=${AUTO_SHUTDOWN_MINUTES}m, resume=${AUTO_RESUME_MINUTES}m`;
      const avg = getMovingAvg(gridPowerHistory);
      const targetsStr = MINER_HOSTS.map(h=>`${h}:${(lastAutoTargets[h] || MIN_POWER)}W`).join(', ') || 'none';
      logger.info(`Auto-control ${autoControlEnabled ? 'ENABLED' : 'DISABLED'}`, { config: cfg, miners: MINER_HOSTS.join(', ') || 'none' });
      logger.info(`Status at toggle: avgGrid=${avg.toFixed(1)}W, targets -> ${targetsStr}`);
    } catch (_) {}

    io.emit('auto_control_update', {
      enabled: autoControlEnabled,
      lastTarget: null,
      lastAvgGridPower: getMovingAvg(gridPowerHistory),
      perMinerTargets: Object.fromEntries(MINER_HOSTS.map(h=>[h, lastAutoTargets[h] || MIN_POWER]))
    });
    res.json({ ok: true, enabled: autoControlEnabled });
  } else {
    res.status(400).json({ error: 'Missing enable boolean' });
  }
});

app.get('/api/auto-control', (req, res) => {
  res.json({
    enabled: autoControlEnabled,
    lastAvgGridPower: getMovingAvg(gridPowerHistory),
    perMinerTargets: Object.fromEntries(MINER_HOSTS.map(h=>[h, lastAutoTargets[h] || MIN_POWER]))
  });
});

app.get('/api/miners', async (req, res) => {
  function normalizeCombined(obj){
    try{
      if (!obj) return { summary: null, devs: [], pools: [] };
      // Case 1: parts => multiple JSON objects
      if (obj.parts && Array.isArray(obj.parts)){
        const parsed = obj.parts.map(p=>{ try{ return JSON.parse(p); } catch{ return null; } }).filter(Boolean);
        const sumHolder = parsed.find(o => o.SUMMARY || o.summary) || {};
        const devHolder = parsed.find(o => o.DEVS || o.devs) || {};
        const poolHolder = parsed.find(o => o.POOLS || o.pools) || {};
        const summaryArr = sumHolder.SUMMARY || sumHolder.summary || [];
        const devsArr = devHolder.DEVS || devHolder.devs || [];
        const poolsArr = poolHolder.POOLS || poolHolder.pools || [];
        return { summary: summaryArr[0] || null, devs: devsArr, pools: poolsArr };
      }
      // Case 2: direct cgminer-like object with arrays
      if (obj.SUMMARY || obj.DEVS || obj.POOLS){
        return { summary: (obj.SUMMARY || [])[0] || null, devs: obj.DEVS || [], pools: obj.POOLS || [] };
      }
      // Case 3: our wrapper style from /api/overview-like
      const wSum = obj?.summary?.[0]?.summary?.[0];
      const wDevs = obj?.devs?.[0]?.devs || [];
      const wPools = obj?.pools?.[0]?.pools || [];
      if (wSum || wDevs.length || wPools.length){
        return { summary: wSum || null, devs: wDevs, pools: wPools };
      }
      return { summary: null, devs: [], pools: [] };
    }catch{
      return { summary: null, devs: [], pools: [] };
    }
  }

  async function fetchAll(host){
    // Try combined first
    let combined;
    try { combined = await papiRequest('summary+devs+pools', host); } catch { combined = null; }
    let { summary, devs, pools } = normalizeCombined(combined);
    if (!summary || !Array.isArray(devs) || !devs.length || !Array.isArray(pools)){
      // Fallback: fetch separately
      const [s, d, p] = await Promise.all([
        papiRequest('summary', host).catch(()=>null),
        papiRequest('devs', host).catch(()=>null),
        papiRequest('pools', host).catch(()=>null),
      ]);
      const pick = (v, keyU, keyL) => {
        if (!v) return null;
        if (v.parts && Array.isArray(v.parts)){
          const obj = JSON.parse(v.parts[0]);
          return obj[keyU] || obj[keyL] || null;
        }
        return v[keyU] || v[keyL] || null;
      };
      const sArr = pick(s, 'SUMMARY', 'summary') || [];
      const dArr = pick(d, 'DEVS', 'devs') || [];
      const pArr = pick(p, 'POOLS', 'pools') || [];
      summary = sArr[0] || summary || null;
      devs = dArr.length ? dArr : (devs || []);
      pools = pArr.length ? pArr : (pools || []);
    }
    return { summary, devs, pools };
  }

  try {
    const results = await Promise.all(
      MINER_HOSTS.map(async (host, idx) => {
        try {
          const { summary, devs, pools } = await fetchAll(host);

          // Calculate totals from devs data
          const totalMHS = (devs || []).reduce((sum, dev) => sum + Number(dev['MHS 5s'] || 0), 0);
          const totalTH = Number.isFinite(totalMHS) ? Number((totalMHS / 1e6).toFixed(2)) : 0;
          
          // Get status and other info
          const status = (devs && devs.length) ? 'Alive' : 'Unknown';
          const uptimeSec = Number(summary?.Elapsed || 0);
          const activePool = (pools || []).find(pool => pool.Status === 'Alive' || /alive/i.test(pool.Status || ''));

          // Fetch power info per miner
          const powerInfo = await getMinerPower(host);
          const totalPower = Number.isFinite(powerInfo.total) ? powerInfo.total : null;

          const maxTemp = (devs && devs.length) ? Math.max(...devs.map(dev => Number(dev.Temperature || 0))) : null;

          return {
            id: idx,
            host,
            status,
            // Keep existing textual uptime for compatibility, but also return seconds
            uptime: fmtUptime(uptimeSec),
            uptimeSec,
            // Existing fields
            totalMH: totalTH, // kept for compatibility (actually TH/s)
            hashrate: `${totalTH} TH/s`,
            // New normalized fields for frontend
            ths: totalTH, // numeric TH/s
            powerW: totalPower, // alias
            totalPower: totalPower,
            totalConsumption: totalPower, // miner consumption approximated by reported power
            version: 'BOSminer',
            grid: 'Connected',
            battery: '—',
            chains: (devs || []).length,
            activeChains: (devs || []).filter(dev => dev.Status === 'Alive').length,
            pools: (pools || []).length,
            activePool: activePool?.URL?.replace('stratum+tcp://', '') || activePool?.URL || 'None',
            accepted: Number(summary?.Accepted || 0),
            rejected: Number(summary?.Rejected || 0),
            hwErrors: Number(summary?.['Hardware Errors'] || 0),
            temperature: maxTemp,
            devs: (devs || []).map(dev => ({
              id: dev.ID,
              status: dev.Status,
              hashrate: ((Number(dev['MHS 5s'] || 0) / 1000000).toFixed(2)) + ' TH/s',
              temperature: dev.Temperature,
              hwErrors: dev['Hardware Errors']
            }))
          };
        } catch (e) {
          return { id: idx, host, error: e.message };
        }
      })
    );
    res.json(results);
  } catch (e) {
    res.status(502).json({ error: e.message });
  }
});

function papiRequest(command, host) {
  return new Promise((resolve, reject) => {
    const client = new net.Socket();
    let data = '';
    let settled = false;

    const settle = (err, val) => { 
      if (settled) return; 
      settled = true; 
      if (err) {
        logger.error('papi socket error', { command, host, error: err.message });
        settle(err);
      } else {
        
        resolve(val);
      }
    };

    client.setEncoding('utf8');
    client.setTimeout(8000);

    client.on('timeout', () => { 
      logger.warn('papi timeout', { command, host });
      client.destroy(new Error('Socket timeout')); 
    });
    client.on('error', (err) => {
      logger.warn('papi socket error', { command, host, error: err.message });
      settle(err);
    });

    client.connect(MINER_PORT, host, () => {
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
        logger.warn('papi invalid JSON preview', { command, host, preview: cleaned.slice(0, 200) });
        return settle(new Error('Invalid JSON from miner'));
      }
    };

    client.on('end', finalize);
    client.on('close', finalize);
  });
}

function splitTopLevelJSONObjects(s) {
  const parts = [];
  let depth = 0, start = -1, inStr = false, esc = false;
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (inStr) {
      if (esc) { esc = false; continue; }
      if (ch === '\\') { esc = true; continue; }
      if (ch === '"') { inStr = false; }
      continue;
    }
    if (ch === '"') { inStr = true; continue; }
    if (ch === '{') { if (depth === 0) start = i; depth++; }
    else if (ch === '}') { depth--; if (depth === 0 && start !== -1) { parts.push(s.slice(start, i + 1)); start = -1; } }
  }
  return parts;
}

function cleanResponse(raw) {
  if (!raw) return '';
  // Remove NUL and other control chars except tab (\t), CR (\r), LF (\n)
  return raw.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, '').trim();
}

function fmtUptime(sec) {
  if (!sec) return '—';
  const d = Math.floor(sec / 86400);
  const h = Math.floor((sec % 86400) / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const parts = [];
  if (d) parts.push(`${d}d`);
  if (h) parts.push(`${h}h`);
  if (m || (!d && !h)) parts.push(`${m}m`);
  return parts.join(' ');
}

// Discrete combo picker for surplus mining (direct best-fit selection)
function pickBestCombo(surplusW, nMiners){
  // Return an array of length nMiners with chosen levels (from ALLOWED_LEVELS) that best match surplusW.
  // Strategy: dynamic programming to build all achievable sums with exactly nMiners picks (repetition allowed),
  // then pick the sum <= surplusW that is maximal; if none exist, pick the sum with minimal absolute difference.
  const allowed = Array.from(new Set(ALLOWED_LEVELS)).slice().sort((a,b)=>a-b);
  if (!nMiners || nMiners <= 0) return [];
  // Fast path for single miner
  if (nMiners === 1) {
    // prefer level <= surplusW and closest; if none, pick closest overall
    let best = allowed[0];
    let bestDiff = Infinity;
    for (const v of allowed) {
      const diff = (v <= surplusW) ? (surplusW - v) : (Math.abs(v - surplusW) + 100000);
      if (diff < bestDiff || (diff === bestDiff && v > best)) { best = v; bestDiff = diff; }
    }
    return [best];
  }

  // dp: Map sum -> combo array (for current count)
  let dp = new Map();
  dp.set(0, []);

  for (let count = 1; count <= nMiners; count++) {
    const next = new Map();
    for (const [sum, combo] of dp.entries()) {
      for (const lvl of allowed) {
        const s = sum + lvl;
        // store the first seen combination for this sum; prefer more balanced combos if collision
        const cand = combo.concat(lvl);
        if (!next.has(s)) {
          next.set(s, cand);
        } else {
          // tie-breaker: prefer combo with smaller range (more balanced)
          const existing = next.get(s);
          const existingRange = Math.max(...existing) - Math.min(...existing);
          const candRange = Math.max(...cand) - Math.min(...cand);
          if (candRange < existingRange) next.set(s, cand);
        }
      }
    }
    dp = next;
    // Safety: if dp size explodes extremely large, cap by trimming unlikely large sums.
    // Keep sums up to (surplusW + maxAllowed) to allow small overshoot choices.
    if (dp.size > 20000) {
      const maxAllowed = allowed[allowed.length-1] * nMiners;
      const cutoff = Math.max(Math.round(surplusW + maxAllowed * 0.2), maxAllowed);
      const entries = Array.from(dp.entries()).sort((a,b)=>Math.abs(a[0]-surplusW) - Math.abs(b[0]-surplusW)).slice(0,10000);
      dp = new Map(entries);
    }
  }

  const sums = Array.from(dp.keys()).sort((a,b)=>a-b);
  // Prefer non-overshoot sums (<= surplusW) with maximal sum
  const nonOver = sums.filter(s => s <= surplusW);
  let bestSum = null;
  let bestCombo = null;
  if (nonOver.length) {
    bestSum = nonOver[nonOver.length - 1];
    bestCombo = dp.get(bestSum);
  } else {
    // pick minimal absolute difference; tie-breaker: larger total (consume more)
    let bestDiff = Infinity;
    for (const s of sums) {
      const diff = Math.abs(s - surplusW);
      if (diff < bestDiff || (diff === bestDiff && s > bestSum)) {
        bestDiff = diff;
        bestSum = s;
        bestCombo = dp.get(s);
      }
    }
  }

  if (!bestCombo) return new Array(nMiners).fill(0);
  // Ensure length exactly nMiners (should be)
  if (bestCombo.length < nMiners) {
    while (bestCombo.length < nMiners) bestCombo.push(0);
  } else if (bestCombo.length > nMiners) {
    bestCombo = bestCombo.slice(0, nMiners);
  }
  return bestCombo;
}

// --- Startup: detect existing miner states (running / stopped + current power_target) ---
async function detectInitialMinerStates(){
  if (!MINER_HOSTS.length) return;
  logger.info('Detecting initial miner states...');
  const tasks = MINER_HOSTS.map(host => new Promise(resolve => {
    // Use single quotes outside and carefully escape inner single quotes for sed/grep
    const sshCmd = `ssh ${SSH_OPTS} root@${host} 'if /etc/init.d/bosminer status >/dev/null 2>&1 || pgrep -x bosminer >/dev/null; then state=RUNNING; else state=STOPPED; fi; pt=$(grep -E "^[[:space:]]*power_target[[:space:]]*=" /etc/bosminer.toml 2>/dev/null | head -1 | sed -E "s/.*=[[:space:]]*([0-9]+).*/\\1/"); echo "$state $pt"'`;
    exec(sshCmd, (err, stdout, stderr) => {
      if (err) {
        logger.warn(`Initial detect failed for ${host}`, { error: stderr || err.message });
        return resolve({ host, state: 'UNKNOWN', power: 0 });
      }
      const out = (stdout || '').trim();
      const parts = out.split(/\s+/);
      const state = parts[0] || 'UNKNOWN';
      const power = parseInt(parts[1], 10);
      resolve({ host, state, power: Number.isFinite(power) ? power : 0 });
    });
  }));

  try {
    const results = await Promise.all(tasks);
    let runningCount = 0;
    for (const r of results){
      if (r.state === 'RUNNING') {
        minerStopped[r.host] = false;
        // If we detect an existing power target keep it; else default MIN_POWER
        lastAutoTargets[r.host] = r.power > 0 ? r.power : MIN_POWER;
        runningCount++;
      } else if (r.state === 'STOPPED') {
        minerStopped[r.host] = true;
        lastAutoTargets[r.host] = 0;
      } else { // unknown
        // Leave defaults; treat as stopped to avoid accidental commands
        minerStopped[r.host] = true;
        if (!Number.isFinite(lastAutoTargets[r.host])) lastAutoTargets[r.host] = 0;
      }
      logger.info(`${r.host} -> ${r.state}${Number.isFinite(r.power) ? ` (power_target ~${r.power}W)` : ''}`);
    }
    minersAreShutDown = runningCount === 0; // if all stopped consider globally shut down
    io.emit('auto_control_update', {
      enabled: autoControlEnabled,
      lastAvgGridPower: getMovingAvg(gridPowerHistory),
      perMinerTargets: Object.fromEntries(MINER_HOSTS.map(h=>[h, lastAutoTargets[h] || 0]))
    });
  } catch (e) {
    logger.warn('Initial detection error', { error: e.message });
  }
}

// Kick off detection shortly after startup (allow env + server init)
setTimeout(()=>{ detectInitialMinerStates(); }, 1500);
// Re-run detection when auto control is enabled the first time if we have no targets yet
function ensureDetectedOnEnable(){
  const missing = MINER_HOSTS.some(h => !Number.isFinite(lastAutoTargets[h]));
  if (missing) detectInitialMinerStates();
}

// Patch auto-control toggle to ensure detection. This middleware must be registered before the route definition below.
app.use('/api/auto-control', (req, res, next) => {
  if (req.method === 'POST' && req.body && typeof req.body.enable === 'boolean' && req.body.enable) {
    ensureDetectedOnEnable();
  }
  next();
});
