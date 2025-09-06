const express = require('express');
const cors = require('cors');
const { exec } = require('child_process');
const mqtt = require('mqtt');
const http = require('http');
const { Server } = require('socket.io');
const net = require('net');
require('dotenv').config();

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

console.log('Connecting to MQTT broker...');
const mqttClient = mqtt.connect({
  host: '192.168.0.50',
  port: 1883,
  protocol: 'mqtt',
});

let latestData = {
  power: { l1: 0, l2: 0, l3: 0 },
  consumption: { l1: 0, l2: 0, l3: 0 }
};

const topics = [
  'N/+/grid/+/Ac/L1/Power',
  'N/+/grid/+/Ac/L2/Power',
  'N/+/grid/+/Ac/L3/Power',
  'N/+/system/+/Ac/ConsumptionOnInput/L1/Power',
  'N/+/system/+/Ac/ConsumptionOnInput/L2/Power',
  'N/+/system/+/Ac/ConsumptionOnInput/L3/Power'
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
  console.log('Connected to MQTT broker');
  mqttClient.subscribe(topics, (err) => {
    if (!err) {
      console.log(`Subscribed to topics: ${topics.join(', ')}`);
    } else {
      console.error(`Subscription error: ${err}`);
      mqttClient.end();
    }
  });
  setInterval(() => {
    mqttClient.publish(`victron/R/${VENUS_ID}/system/0/Serial`, '');
    // console.log('Sent keep-alive to victron/R/' + VENUS_ID + '/system/0/Serial');
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
    }
  } catch (e) {
    console.error(`Could not parse message on topic ${topic}: ${message.toString()}`);
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
    timestamp: new Date().toISOString()
  };
  io.emit('data_update', dataToSend);
  latestData.lastSent = dataToSend;
}, 5000);

mqttClient.on('error', (err) => {
  console.error(`Connection error: ${err}`);
  mqttClient.end();
});

mqttClient.on('close', () => {
  console.log('Connection to MQTT broker closed');
});

// Optional request logging (disabled by default). To enable: REQUEST_LOG=true
app.use((req, res, next) => {
  if (REQUEST_LOG && !REQUEST_LOG_IGNORE_PREFIX.some(p => p && req.url.startsWith(p))) {
    console.log(`[${new Date().toISOString()}] ${req.method} ${req.url} from ${req.headers.origin || 'unknown'}`);
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
    const sshCmd = `ssh root@${minerHost} "/etc/init.d/bosminer stop"`;
    exec(sshCmd, (err, stdout, stderr) => {
      if (err) return res.status(500).json({ error: stderr || err.message });
      res.json({ ok: true, shutdown: true });
    });
    return;
  }
  if (req.body.resume) {
    // SSH command to start BOSminer
    const sshCmd = `ssh root@${minerHost} "/etc/init.d/bosminer start"`;
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
  const sshCmd = `ssh root@${minerHost} "sed -i -E 's/^([[:space:]]*power_target[[:space:]]*=[[:space:]]*)[0-9]+/\\1 ${watts}/' /etc/bosminer.toml && /etc/init.d/bosminer reload"`;

  exec(sshCmd, (err, stdout, stderr) => {
    if (err) return res.status(500).json({ error: stderr || err.message });
    res.json({ ok: true, watts });
  });
});

// --- Solar surplus miner control logic ---
const SURPLUS_CHECK_INTERVAL = 60000; // ms
const MOVING_AVG_WINDOW = 20; // window length for moving avg of grid power samples
const MIN_POWER = 0; // W per miner when idling/paused
const MAX_POWER = 1420; // W per miner maximum target
const GRID_SURPLUS_THRESHOLD = -100; // W (negative = exporting to grid => surplus)
const GRID_DEFICIT_THRESHOLD = 100; // W (positive = importing from grid)
const HYSTERESIS = 50; // W noise band
const MAX_STEP_CHANGE = 150; // W, limit change per interval per miner to avoid thrashing
const PER_MINER_MIN_APPLY_INTERVAL_MS = 60000; // ms

// Discrete two-miner combo control
const DISCRETE_MODE = true; // enable combo-based control for 2 miners
const ALLOWED_LEVELS = [0, 800, 1000, 1420];
const ENTER_MARGIN = 0; // no step-up margin (direct jumps allowed)
const EXIT_MARGIN = 0;  // no step-down hysteresis (can jump directly)
// Removed STRICT_STEPS logic (direct selection now)

let gridPowerHistory = [];
let lastAutoSet = 0;
let autoControlEnabled = false; // can be toggled via UI later

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
      const sshCmd = `ssh root@${host} "/etc/init.d/bosminer stop"`;
      exec(sshCmd, (err, stdout, stderr) => {
        if (err) {
          console.error(`[AUTO] Stop error on ${host}: ${stderr || err.message}`);
          return resolve({ host, error: stderr || err.message });
        }
        minerStopped[host] = true;
        lastAutoTargets[host] = 0;
        lastApplyTs[host] = now;
        console.log(`[AUTO] Set ${host} -> OFF (was ${prev}W)`);
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
    const sshCmd = `ssh root@${host} "${parts.join(' && ')}"`;

    exec(sshCmd, (err, stdout, stderr) => {
      if (err) {
        console.error(`[AUTO] SSH error on ${host}: ${stderr || err.message}`);
        return resolve({ host, error: stderr || err.message });
      }
      lastAutoTargets[host] = logicalTarget;
      lastApplyTs[host] = now;
      console.log(`[AUTO] Set ${host} -> ${logicalTarget}W (was ${prev}W${prev===0?' OFF':''})`);
      resolve({ host, applied: logicalTarget });
    });
  });
}

// Helper to stop all miners (BOSminer stop)
async function shutdownAllMiners(){
  const tasks = MINER_HOSTS.map(host => new Promise((resolve) => {
    const sshCmd = `ssh root@${host} "/etc/init.d/bosminer stop"`;
    exec(sshCmd, (err) => resolve({ host, ok: !err }));
  }));
  return Promise.allSettled(tasks);
}

// Helper to start all miners (BOSminer start)
async function startAllMiners(){
  const tasks = MINER_HOSTS.map(host => new Promise((resolve) => {
    const sshCmd = `ssh root@${host} "/etc/init.d/bosminer start"`;
    exec(sshCmd, (err) => resolve({ host, ok: !err }));
  }));
  return Promise.allSettled(tasks);
}

function autoSetMinerPower() {
  if (!autoControlEnabled) {
    console.log(`[AUTO] Auto-control is disabled.`);
    return;
  }
  const gridPower = latestData.power.l1 + latestData.power.l2 + latestData.power.l3; // positive = importing, negative = exporting
  gridPowerHistory.push(gridPower);
  if (gridPowerHistory.length > MOVING_AVG_WINDOW) gridPowerHistory.shift();
  const avgGridPower = getMovingAvg(gridPowerHistory);
  const surplusW = Math.max(0, -avgGridPower);
  const now = Date.now();
  // Only act every interval
  if (now - lastAutoSet < SURPLUS_CHECK_INTERVAL) {
    console.log(`[AUTO] Waiting for next interval. Last set: ${new Date(lastAutoSet).toISOString()}`);
    return;
  }
  lastAutoSet = now;

  if (!MINER_HOSTS.length) {
    console.warn('[AUTO] No miners configured in MINER_HOSTS');
    return;
  }

  // Ensure we have initial targets
  for (const host of MINER_HOSTS) if (!Number.isFinite(lastAutoTargets[host])) lastAutoTargets[host] = MIN_POWER;

  // --- Auto shutdown / resume ---
  const shutdownEnabled = AUTO_SHUTDOWN_MINUTES > 0;
  const resumeEnabled = AUTO_RESUME_MINUTES > 0;

  // If miners are currently stopped, only evaluate resume condition and skip power targeting
  if (minersAreShutDown) {
    if (resumeEnabled) {
      const resumeCond = gridPower < (GRID_SURPLUS_THRESHOLD - HYSTERESIS); // instantaneous surplus export sustained
      if (resumeCond) {
        if (!surplusHoldStartTs) {
          surplusHoldStartTs = now;
          console.log(`[AUTO] Resume arming: sustained surplus detected. Waiting ${AUTO_RESUME_MINUTES} min...`);
        }
        const heldMs = now - (surplusHoldStartTs || now);
        if (heldMs >= AUTO_RESUME_MINUTES * 60000) {
          console.log('[AUTO] Auto-resume: starting all miners.');
          startAllMiners().then(() => {
            minersAreShutDown = false;
            surplusHoldStartTs = null;
            noSurplusStartTs = null;
            // Reset targets to MIN_POWER so control can ramp up cleanly next cycles
            for (const h of MINER_HOSTS) { lastAutoTargets[h] = MIN_POWER; lastApplyTs[h] = 0; }
            io.emit('auto_control_update', { enabled: autoControlEnabled, autoEvent: 'resumed', lastAvgGridPower: avgGridPower });
          });
        }
      } else if (surplusHoldStartTs) {
        console.log('[AUTO] Resume condition cleared.');
        surplusHoldStartTs = null;
      }
    }
    // While shut down, skip target application
    return;
  }

  // Miners running: evaluate shutdown condition
  if (shutdownEnabled) {
    // Treat instantaneous "no surplus" (gridPower >= 0) or explicit importing beyond threshold as shutdown condition
    const noSurplus = gridPower >= 0;
    const importing = gridPower > (GRID_DEFICIT_THRESHOLD + HYSTERESIS);
    const shutdownCond = noSurplus || importing;
    if (shutdownCond) {
      if (!noSurplusStartTs) {
        noSurplusStartTs = now;
        console.log(`[AUTO] Shutdown arming: no-surplus/deficit detected. Waiting ${AUTO_SHUTDOWN_MINUTES} min...`);
      }
      const heldMs = now - (noSurplusStartTs || now);
      if (heldMs >= AUTO_SHUTDOWN_MINUTES * 60000) {
        console.log('[AUTO] Auto-shutdown: stopping all miners.');
        shutdownAllMiners().then(() => {
          minersAreShutDown = true;
          noSurplusStartTs = null;
          surplusHoldStartTs = null;
          io.emit('auto_control_update', { enabled: autoControlEnabled, autoEvent: 'shutdown', lastAvgGridPower: avgGridPower });
        });
        return; // Do not proceed with targeting after issuing shutdown
      }
    } else if (noSurplusStartTs) {
      console.log('[AUTO] Shutdown condition cleared.');
      noSurplusStartTs = null;
    }
  }

  // Discrete two-miner mode
  if (DISCRETE_MODE && MINER_HOSTS.length === 2) {
    // Compute desired combo directly (no stepwise hysteresis)
    const nextCombo = pickBestCombo(Math.max(0, -avgGridPower));

    const actions = [];
    for (let i = 0; i < MINER_HOSTS.length; i++) {
      const host = MINER_HOSTS[i];
      const desired = nextCombo[i];
      actions.push(applyMinerPower(host, desired));
    }

    Promise.allSettled(actions).then(()=>{
      io.emit('auto_control_update', {
        enabled: autoControlEnabled,
        lastAvgGridPower: avgGridPower,
        surplusW: Math.max(0, -avgGridPower),
        discreteCombo: nextCombo,
        perMinerTargets: Object.fromEntries(MINER_HOSTS.map(h=>[h, lastAutoTargets[h]||0]))
      });
    });
    return;
  }

  // Continuous control path
  const hostsFair = reorderHostsFair(MINER_HOSTS);
  const actions = [];
  if (avgGridPower < GRID_SURPLUS_THRESHOLD - HYSTERESIS) {
    // Excess solar: increase miners to absorb surplus
    let remaining = Math.round(-avgGridPower); // W to consume additionally
    // Compute current sum and max additional capacity
    let currentSum = 0, maxCapacity = 0;
    for (const host of hostsFair) {
      const cur = lastAutoTargets[host];
      currentSum += cur;
      maxCapacity += Math.max(0, MAX_POWER - cur);
    }
    if (remaining > maxCapacity) remaining = maxCapacity; // cap at available headroom

    console.log(`[AUTO] Surplus ${(-avgGridPower).toFixed(0)}W (avg). Distributing +${remaining}W across ${hostsFair.length} miners.`);

    for (const host of hostsFair) {
      if (remaining <= 0) break;
      const cur = lastAutoTargets[host];
      const headroom = Math.max(0, MAX_POWER - cur);
      const inc = Math.min(headroom, Math.ceil(remaining / Math.max(1, hostsFair.length))); // fair-ish share
      const next = clamp(cur + inc, MIN_POWER, MAX_POWER);
      actions.push(applyMinerPower(host, next));
      remaining -= (next - cur);
    }
  } else if (avgGridPower > GRID_DEFICIT_THRESHOLD + HYSTERESIS) {
    // Importing from grid: reduce miners toward MIN_POWER
    let needReduce = Math.round(avgGridPower); // W to reduce
    // Sort hosts by current target descending to reduce highest first
    const hostsByPower = hostsFair.slice().sort((a,b)=> (lastAutoTargets[b]||MIN_POWER) - (lastAutoTargets[a]||MIN_POWER));

    let reducibleTotal = 0;
    for (const h of hostsByPower) reducibleTotal += Math.max(0, (lastAutoTargets[h]||MIN_POWER) - MIN_POWER);
    if (needReduce > reducibleTotal) needReduce = reducibleTotal;

    console.log(`[AUTO] Deficit ${avgGridPower.toFixed(0)}W (avg). Reducing −${needReduce}W across ${hostsFair.length} miners.`);

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
    console.log(`[AUTO] Within hysteresis. Avg grid: ${avgGridPower.toFixed(1)}W. No changes.`);
  }

  if (actions.length) {
    Promise.allSettled(actions).then((results)=>{
      const summary = results.map(r=> r.value?.applied ? `${r.value.host}:${r.value.applied}W` : `${r.value?.host||'?'}:skip`).join(', ');
      io.emit('auto_control_update', {
        enabled: autoControlEnabled,
        lastAvgGridPower: avgGridPower,
        perMinerTargets: Object.fromEntries(MINER_HOSTS.map(h=>[h, lastAutoTargets[h]]))
      });
      console.log(`[AUTO] Applied: ${summary}`);
    });
  } else {
    io.emit('auto_control_update', {
      enabled: autoControlEnabled,
      lastAvgGridPower: avgGridPower,
      perMinerTargets: Object.fromEntries(MINER_HOSTS.map(h=>[h, lastAutoTargets[h]]))
    });
  }
}
setInterval(autoSetMinerPower, SURPLUS_CHECK_INTERVAL);

const PORT = process.env.PORT || 3099;
server.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});

process.on('SIGINT', () => {
  console.log('Disconnecting from MQTT broker...');
  mqttClient.end();
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
      console.log(`[AUTO] Auto-control ${autoControlEnabled ? 'ENABLED' : 'DISABLED'} (${cfg}). Miners: ${MINER_HOSTS.join(', ') || 'none'}`);
      console.log(`[AUTO] Status at toggle: avgGrid=${avg.toFixed(1)}W, targets -> ${targetsStr}`);
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
        console.log(`[PAPI] Error for ${command} from ${host}: ${err.message}`);
        reject(err);
      } else {
        
        resolve(val);
      }
    };

    client.setEncoding('utf8');
    client.setTimeout(8000);

    client.on('timeout', () => { 
      console.log(`[PAPI] Timeout for ${command} from ${host}`);
      client.destroy(new Error('Socket timeout')); 
    });
    client.on('error', (err) => {
      console.log(`[PAPI] Socket error for ${command} from ${host}: ${err.message}`);
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
        console.error('[papi] Invalid JSON preview:', cleaned.slice(0, 200));
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
function pickBestCombo(surplusW){
  const combos = [];
  for (const a of ALLOWED_LEVELS) for (const b of ALLOWED_LEVELS) combos.push([a,b]);
  let best = [0,0];
  let bestScore = Infinity; // lower better
  for (const c of combos){
    const total = c[0]+c[1];
    const diff = surplusW - total; // positive means unused surplus, negative means overshoot
    // Primary: prefer non-overshoot (diff>=0). Among those pick smallest diff.
    // If all overshoot, pick smallest absolute diff.
    let score;
    if (diff >= 0) score = diff; else score = Math.abs(diff) + 100000; // large penalty for overshoot
    // Tie-breakers: larger total (consume more surplus), then more balanced (lower |a-b|)
    if (score < bestScore || (score === bestScore && (total > (best[0]+best[1]) || (total === (best[0]+best[1]) && Math.abs(c[0]-c[1]) < Math.abs(best[0]-best[1])))) ){
      best = c;
      bestScore = score;
    }
  }
  return best;
}
