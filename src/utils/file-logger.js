const fs = require('fs');
const path = require('path');

/**
 * Simple rotating file logger.
 * - Writes human-readable lines to file.
 * - Creates logs directory if missing.
 * - Daily rotation (YYYY-MM-DD.log) by default.
 * - Optional console mirroring.
 */
class FileLogger {
  /**
   * @param {object} opts
   * @param {string} [opts.dir] Directory for logs (default: ./logs)
   * @param {string} [opts.prefix] File prefix (default: app)
   * @param {boolean} [opts.console] Also log to console
   */
  constructor(opts = {}) {
    this.dir = path.resolve(opts.dir || path.join(process.cwd(), 'logs'));
    this.prefix = opts.prefix || 'app';
    this.mirrorConsole = !!opts.console;
    this.stream = null;
    this.currentDate = null;
    this.levels = ['debug', 'info', 'warn', 'error'];
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
      this.close();
      this.ensureDir();
      const fp = this.filePathFor(Date.now());
      this.stream = fs.createWriteStream(fp, { flags: 'a', encoding: 'utf8' });
      this.currentDate = today;
    }
  }

  line(level, msg, meta) {
    try {
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
          // ignore meta stringify errors
        }
      }
      this.stream?.write(`${line}\n`);
      if (this.mirrorConsole) {
        /* eslint-disable no-console -- intentional console mirror as a last-resort fallback */
        let fn;
        if (level === 'error') fn = console.error;
        else if (level === 'warn') fn = console.warn;
        else fn = console.log;
        fn(line);
        /* eslint-enable no-console */
      }
    } catch (e) {
      // last resort
      try {
        /* eslint-disable-next-line no-console -- last-resort fallback if file writes fail */
        console.error('Logger write failed:', e);
      } catch (e2) {
        // ignore secondary logger errors
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
      this.stream?.end();
    } catch (e) {
      // ignore stream close errors
    }
    this.stream = null;
  }
}

module.exports = { FileLogger };
