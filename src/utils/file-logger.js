const fs = require('fs');
const path = require('path');
const pino = require('pino');

/**
 * Adapter-file logger using pino for structured output and a fallback human-readable file stream.
 * Maintains the original FileLogger API: debug/info/warn/error, close()
 */
class FileLogger {
  constructor(opts = {}) {
    this.dir = path.resolve(opts.dir || path.join(process.cwd(), 'logs'));
    this.prefix = opts.prefix || 'app';
    this.mirrorConsole = !!opts.console;
    this.stream = null;
    this.currentDate = null;
    this.levels = ['debug', 'info', 'warn', 'error'];

    const pinoOpts = { level: opts.level || process.env.LOG_LEVEL || 'info' };
    this.logger = pino(pinoOpts);
  }

  ensureDir() {
    fs.mkdirSync(this.dir, { recursive: true });
  }

  filePathFor(date) {
    const d = new Date(date);
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return path.join(this.dir, `${this.prefix}-${y}-${m}-${day}.log`);
  }

  rotateIfNeeded() {
    const today = new Date().toDateString();
    if (this.currentDate !== today) {
      try {
        if (this.stream) this.stream.end();
      } catch (e) {
        // ignore
      }
      this.stream = null;
      this.ensureDir();
      const fp = this.filePathFor(Date.now());
      this.stream = fs.createWriteStream(fp, { flags: 'a', encoding: 'utf8' });
      this.currentDate = today;
    }
  }

  line(level, msg, meta) {
    try {
      // Structured pino log
      if (meta !== undefined) this.logger[level]({ meta }, msg);
      else this.logger[level](msg);

      // Human-readable file fallback (non-blocking)
      this.rotateIfNeeded();
      const now = new Date();
      const ts = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(
        now.getDate()
      ).padStart(2, '0')}T${String(now.getHours()).padStart(2, '0')}:${String(
        now.getMinutes()
      ).padStart(2, '0')}:${String(now.getSeconds()).padStart(2, '0')}.${String(
        now.getMilliseconds()
      ).padStart(3, '0')}`;
      let line = `[${ts}] [${level.toUpperCase()}] ${msg}`;
      if (meta !== undefined) {
        try {
          line += ` ${typeof meta === 'string' ? meta : JSON.stringify(meta)}`;
        } catch (e) {
          line += ' [meta-stringify-error]';
        }
      }
      this.stream?.write(`${line}\n`);

      // mirrorConsole is supported by writing structured logs to stdout (pino),
      // so no extra console.* calls are necessary.
    } catch (e) {
      try {
        // eslint-disable-next-line no-console -- last-resort fallback
        console.error('Logger failed', e);
      } catch (_e) {
        // ignore
      }
    }
  }

  debug(msg, meta) {
    this.line('debug', msg, meta);
  }

  info(msg, meta) {
    this.line('info', msg, meta);
  }

  warn(msg, meta) {
    this.line('warn', msg, meta);
  }

  error(msg, meta) {
    this.line('error', msg, meta);
  }

  close() {
    try {
      if (this.stream) this.stream.end();
    } catch (e) {
      // ignore
    }
    this.stream = null;
  }
}

module.exports = { FileLogger };
