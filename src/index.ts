#!/usr/bin/env node
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import Database from "better-sqlite3";
import { createWriteStream, readFileSync, statSync, realpathSync } from "node:fs";
import { resolve, relative, isAbsolute, dirname, basename } from "node:path";
import { createRequire } from "node:module";
import { parse as parseCsv } from "csv-parse/sync";
import { stringify as stringifyCsv } from "csv-stringify/sync";

type Handle = {
  name: string;
  path: string;
  db: Database.Database;
  readonly: boolean;
  opened_at: number;
};

const registry = new Map<string, Handle>();
const DEFAULT_NAME = "default";

// B7: track in-flight async ops (export_csv / export_json / backup) so shutdown
// can wait for them to drain before closing DB handles.
let inflightAsync = 0;
async function trackAsync<T>(fn: () => Promise<T>): Promise<T> {
  inflightAsync++;
  try {
    return await fn();
  } finally {
    inflightAsync--;
  }
}

// B1: Guard NaN/negative on env-var parses.
function parseIntSafe(s: string | undefined, fallback: number): number {
  if (s === undefined) return fallback;
  const n = parseInt(s, 10);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

// S4: hard cap; configurable via env, default 100000.
const MAX_LIMIT = parseIntSafe(process.env.SQLITE_MAX_LIMIT, 100_000);
// B4: clamp MAX_ROWS to MAX_LIMIT so MAX_ROWS > MAX_LIMIT is impossible.
const MAX_ROWS = Math.min(parseIntSafe(process.env.SQLITE_MAX_ROWS, 1000), MAX_LIMIT);
const ALLOW_DANGEROUS = process.env.SQLITE_ALLOW_DANGEROUS === "1";

// S1: confine FS-touching paths to a root. Default: server CWD.
// S2: resolve symlinks at startup so attackers can't escape via symlinked roots.
const ALLOWED_ROOT_RAW = resolve(process.env.SQLITE_ALLOWED_ROOT ?? process.cwd());
const ALLOWED_ROOT = (() => {
  try {
    return realpathSync(ALLOWED_ROOT_RAW);
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code === "ENOENT") return ALLOWED_ROOT_RAW;
    throw e;
  }
})();

// Read package version once for server identity (B10).
const pkgRequire = createRequire(import.meta.url);
let PKG_VERSION = "0.0.0";
try {
  PKG_VERSION = (pkgRequire("../package.json") as { version?: string }).version ?? "0.0.0";
} catch {
  try {
    PKG_VERSION = (pkgRequire("../../package.json") as { version?: string }).version ?? "0.0.0";
  } catch {}
}

// S1 helper: resolve the deepest existing ancestor so symlinks in any prefix
// of a non-existent write path are still caught.
function realpathBestEffort(p: string): string {
  try { return realpathSync(p); } catch (e) {
    if ((e as NodeJS.ErrnoException).code !== "ENOENT") throw e;
  }
  const parent = dirname(p);
  if (parent === p) return p;
  return resolve(realpathBestEffort(parent), basename(p));
}

// S1 helper: validate that an absolute path is within ALLOWED_ROOT (no string startsWith).
// S2: realpath the candidate first so symlinks-escape is caught. Write destinations
// may not exist yet — realpathBestEffort walks ancestors to catch symlinks in parent chain.
// U9: error message must NOT include ALLOWED_ROOT (info leak); generic only.
function ensureWithinAllowedRoot(absPath: string): void {
  const abs = realpathBestEffort(resolve(absPath));
  const rel = relative(ALLOWED_ROOT, abs);
  if (rel === "" || (!rel.startsWith("..") && !isAbsolute(rel))) return;
  throw new Error(
    `path is outside the allowed root. ` +
    `Set SQLITE_ALLOWED_ROOT to permit access.`,
  );
}

// S7 helper: reject NUL / CR / LF and all other control chars (incl. DEL) in user-supplied paths.
function sanitizePath(p: string, label: string): string {
  if (typeof p !== "string" || p.length === 0) throw new Error(`${label}: empty path`);
  if (/[\x00-\x1f\x7f]/.test(p)) throw new Error(`${label}: path contains control character`);
  return p;
}

// Resolve + sanitize + root-confine a user-supplied path.
function resolveSafePath(p: string, label: string): string {
  sanitizePath(p, label);
  const abs = resolve(p);
  ensureWithinAllowedRoot(abs);
  return abs;
}

// S2: SQLite identifier quoting.
// S6: hard 1000-char cap to prevent pathological identifier names.
function quoteIdent(name: string): string {
  if (typeof name !== "string" || !/^[A-Za-z_][A-Za-z0-9_$]*$/.test(name)) {
    throw new Error(`invalid SQL identifier: ${JSON.stringify(name)}`);
  }
  if (name.length > 1000) {
    throw new Error(`SQL identifier too long (${name.length} > 1000): ${JSON.stringify(name.slice(0, 64) + "…")}`);
  }
  return `"${name.replace(/"/g, '""')}"`;
}

// S9: schema-name validation for ATTACH.
function validateSchemaName(name: string): string {
  if (typeof name !== "string" || !/^[A-Za-z_][A-Za-z0-9_]{0,62}$/.test(name)) {
    throw new Error(`invalid schema_name: ${JSON.stringify(name)}`);
  }
  return name;
}

// S3: PRAGMA allowlist. Read PRAGMAs allowed; some can also be assigned.
const PRAGMA_ALLOWLIST = new Set<string>([
  "journal_mode",
  "synchronous",
  "cache_size",
  "foreign_keys",
  "table_info",
  "foreign_key_list",
  "index_list",
  "busy_timeout",
  "recursion_limit",
  "max_recursion_depth",
  "page_size",
  "schema_version",
  "user_version",
  "integrity_check",
  "quick_check",
  "table_xinfo",
  "index_info",
  "function_list",
  // Used internally + in README examples; safe and useful to inspect.
  "table_list",
  "database_list",
]);

// PRAGMAs we always block, even with ALLOW_DANGEROUS=1, because they enable
// arbitrary file writes / schema corruption that defeats the SQL-injection guards.
const PRAGMA_BLOCKLIST = new Set<string>([
  "writable_schema",
  "temp_store_directory",
  "data_store_directory",
]);

function extractPragmaName(expr: string): string {
  // Accept "name", "name = value", "name(arg)", "schema.name", etc.
  // Pull the leading bare-word identifier and lowercase it.
  const m = /^\s*(?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)?([A-Za-z_][A-Za-z0-9_]*)/.exec(expr);
  if (!m) throw new Error(`invalid pragma expression: ${JSON.stringify(expr)}`);
  return m[1]!.toLowerCase();
}

function ensurePragmaAllowed(expr: string): void {
  if (/;/.test(expr)) throw new Error("pragma expression must not contain semicolons");
  const name = extractPragmaName(expr);
  if (PRAGMA_BLOCKLIST.has(name)) {
    throw new Error(`pragma "${name}" is blocked (writes outside SQL boundary)`);
  }
  if (!PRAGMA_ALLOWLIST.has(name) && !ALLOW_DANGEROUS) {
    throw new Error(
      `pragma "${name}" is not on the allowlist. ` +
      `Set SQLITE_ALLOW_DANGEROUS=1 to permit, or use one of: ${[...PRAGMA_ALLOWLIST].join(", ")}.`,
    );
  }
}

// S8: scan execute_script for dangerous statements.
function ensureScriptSafe(sql: string): void {
  if (ALLOW_DANGEROUS) return;
  // Replace block comments with a single space (not empty string) so that
  // "ATTACH/**/DATABASE" doesn't collapse into "ATTACHDATABASE" and evade the keyword regex.
  // S1: line comments must also collapse to a single space, otherwise
  //   ATTACH--evil\nDATABASE  becomes  ATTACHDATABASE  (no whitespace) and \bATTACH\b misses.
  const stripped = sql
    .replace(/\/\*[\s\S]*?\*\//g, " ")
    .replace(/--[^\n]*/g, " ");
  // SQLite grammar: ATTACH [DATABASE] expr AS name  — DATABASE keyword is optional.
  if (/\bATTACH\b/i.test(stripped)) {
    throw new Error(
      "execute_script: ATTACH is forbidden in scripts. " +
      "Use action=attach instead, or set SQLITE_ALLOW_DANGEROUS=1.",
    );
  }
  // S4: scan EVERY `PRAGMA <name>` occurrence and run it through the blocklist,
  // not just `writable_schema`. This catches `temp_store_directory` etc. that
  // were previously accepted in scripts.
  const pragmaRe = /\bPRAGMA\s+((?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)?[A-Za-z_][A-Za-z0-9_]*)/gi;
  let m: RegExpExecArray | null;
  while ((m = pragmaRe.exec(stripped)) !== null) {
    const name = extractPragmaName(m[1]!);
    if (PRAGMA_BLOCKLIST.has(name)) {
      throw new Error(
        `execute_script: PRAGMA ${name} is blocked.`,
      );
    }
    if (!PRAGMA_ALLOWLIST.has(name) && !ALLOW_DANGEROUS) {
      throw new Error(
        `execute_script: PRAGMA ${name} is not on the allowlist. ` +
        `Set SQLITE_ALLOW_DANGEROUS=1 to permit, or use one of: ${[...PRAGMA_ALLOWLIST].join(", ")}.`,
      );
    }
  }
}

// C1: bind dispatch helpers. better-sqlite3's stmt.run/all/get/iterate take
// EITHER spread positional args (array → ...spread) OR a single named-bind
// object (Record<string, unknown> → pass as one arg). Passing the array
// without spread silently fails for named binds. Centralize the dispatch.
type BindParams = unknown[] | Record<string, unknown> | undefined;
function callRun(stmt: Database.Statement, params: BindParams): Database.RunResult {
  if (params === undefined) return stmt.run();
  if (Array.isArray(params)) return stmt.run(...params);
  return stmt.run(params);
}
function callAll(stmt: Database.Statement, params: BindParams): unknown[] {
  if (params === undefined) return stmt.all();
  if (Array.isArray(params)) return stmt.all(...params);
  return stmt.all(params);
}
function callGet(stmt: Database.Statement, params: BindParams): unknown {
  if (params === undefined) return stmt.get();
  if (Array.isArray(params)) return stmt.get(...params);
  return stmt.get(params);
}
function callIterate(stmt: Database.Statement, params: BindParams): IterableIterator<unknown> {
  if (params === undefined) return stmt.iterate() as IterableIterator<unknown>;
  if (Array.isArray(params)) return stmt.iterate(...params) as IterableIterator<unknown>;
  return stmt.iterate(params) as IterableIterator<unknown>;
}

// Some LLM tool-call paths deliver object/array arguments as JSON strings.
// Parse those back into structured values before use.
function coerceObject<T>(v: unknown): T | undefined {
  if (v === undefined || v === null) return undefined;
  if (typeof v === "string") {
    const s = v.trim();
    if (s === "") return undefined;
    try {
      return JSON.parse(s) as T;
    } catch (e) {
      throw new Error(`failed to parse JSON string argument: ${e instanceof Error ? e.message : String(e)}`);
    }
  }
  return v as T;
}

function getHandle(name?: string): Handle {
  const n = name ?? DEFAULT_NAME;
  const h = registry.get(n);
  if (!h) {
    // U10: nicer message when caller never specified a name.
    if (name === undefined) {
      throw new Error(`default db not open. Call action=open first.`);
    }
    throw new Error(`no db open for name="${n}". Call action=open first.`);
  }
  return h;
}

// B8: serialize values that JSON.stringify can't represent natively.
// Buffer (BLOB) → { $blob: <base64> } so callers can round-trip back.
function serializeValue(v: unknown): unknown {
  if (v === null || v === undefined) return v;
  if (Buffer.isBuffer(v)) return { $blob: v.toString("base64") };
  if (typeof v === "bigint") return Number(v);
  return v;
}
function serializeRow(row: unknown): unknown {
  if (row === null || row === undefined) return row;
  if (Array.isArray(row)) return row.map(serializeValue);
  if (Buffer.isBuffer(row)) return serializeValue(row);
  if (typeof row === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(row as Record<string, unknown>)) {
      out[k] = serializeValue(v);
    }
    return out;
  }
  return row;
}
// CSV needs a string-shaped repr (no nested object).
function blobToCsvCell(v: unknown): unknown {
  if (Buffer.isBuffer(v)) return `data:application/octet-stream;base64,${v.toString("base64")}`;
  return v;
}

function openDb(p: {
  path: string;
  name?: string;
  readonly?: boolean;
  create?: boolean;
  pragmas?: string[];
}): Handle {
  const n = p.name ?? DEFAULT_NAME;
  const existing = registry.get(n);
  if (existing) {
    try { existing.db.close(); } catch {}
    registry.delete(n);
  }
  const abs = resolveSafePath(p.path, "open.path");
  const readonly = p.readonly === true;
  // B7: drop existsSync precheck — better-sqlite3's fileMustExist handles it atomically.
  // C2: readonly mode implies the file must exist; SQLite would otherwise create a
  // 0-byte unusable read-only handle.
  const db = new Database(abs, {
    readonly,
    fileMustExist: p.create === false || readonly === true,
  });

  // S6: every opened DB gets sane busy/recursion settings.
  try { db.pragma("busy_timeout = 5000"); } catch {}
  try { db.pragma("max_recursion_depth = 1000"); } catch {}

  for (const pr of p.pragmas ?? []) {
    try {
      ensurePragmaAllowed(pr);
      db.pragma(pr);
    } catch (e: unknown) {
      // U5: narrow `unknown` properly instead of `any`.
      try { db.close(); } catch {}
      const msg = e instanceof Error ? e.message : String(e);
      throw new Error(`invalid pragma "${pr}": ${msg}`);
    }
  }
  const h: Handle = { name: n, path: abs, db, readonly, opened_at: Date.now() };
  registry.set(n, h);
  return h;
}

function closeDb(name?: string): { ok: true; closed: true; name: string } {
  const n = name ?? DEFAULT_NAME;
  const h = registry.get(n);
  // U2: if no such handle, the caller should be told via errContent — caller checks
  // for `null` and converts to errContent. Don't silently report success.
  if (!h) {
    throw new Error(`no db open for name="${n}"`);
  }
  try { h.db.close(); } catch {}
  registry.delete(n);
  return { ok: true, closed: true, name: n };
}

function closeAllDbs() {
  for (const [n, h] of [...registry]) {
    try { h.db.close(); } catch {}
    registry.delete(n);
  }
}

function runQuery(h: Handle, sql: string, params?: unknown[] | Record<string, unknown>, limit?: number) {
  // S4: cap limit.
  const requested = limit ?? MAX_ROWS;
  if (requested > MAX_LIMIT) {
    throw new Error(`limit ${requested} exceeds SQLITE_MAX_LIMIT=${MAX_LIMIT}`);
  }
  const lim = requested;
  const stmt = h.db.prepare(sql);

  // B2: stmt.columns() throws on non-SELECT; only call for readers.
  let columns: Array<{ name: string; type: string | null; column: string | null; table: string | null }> = [];
  if (stmt.reader) {
    try {
      columns = stmt.columns().map((c) => ({ name: c.name, type: c.type, column: c.column, table: c.table }));
    } catch {
      columns = [];
    }
  }

  // S5: stream rows up to lim+1 to detect truncation without loading everything.
  // C1: callIterate handles named-bind objects correctly.
  // B1: drop manual iter.return(); for-of's auto-close handles it.
  // B8: serialize BLOBs/bigints in each row.
  const kept: unknown[] = [];
  let truncated = false;
  if (stmt.reader) {
    const iter = callIterate(stmt, params);
    try {
      for (const row of iter) {
        if (kept.length >= lim) {
          truncated = true;
          break;
        }
        kept.push(serializeRow(row));
      }
    } finally {
      try { iter.return?.(); } catch {}
    }
  } else {
    // Non-SELECT prepared with .prepare() — fall back to .all() which returns [].
    const rows = callAll(stmt, params);
    for (const r of rows.slice(0, lim)) kept.push(serializeRow(r));
  }

  return {
    row_count: kept.length,
    rows: kept,
    truncated,
    limit: lim,
    columns,
  };
}

function runExec(h: Handle, sql: string, params?: unknown[] | Record<string, unknown>) {
  if (h.readonly) throw new Error(`db "${h.name}" is readonly`);
  let stmt: Database.Statement;
  try {
    stmt = h.db.prepare(sql);
  } catch (e) {
    // B3: friendlier hint for the common "more than one statement" footgun.
    if (e instanceof Error && /more than one statement/i.test(e.message)) {
      throw new Error(
        `${e.message}. Hint: action=execute runs a single prepared statement; use action=execute_script for multi-statement scripts.`,
      );
    }
    throw e;
  }
  // C1: callRun handles named-bind objects correctly.
  const info = callRun(stmt, params);
  return {
    changes: info.changes,
    last_insert_rowid: Number(info.lastInsertRowid),
  };
}

function runExecScript(h: Handle, sql: string) {
  if (h.readonly) throw new Error(`db "${h.name}" is readonly`);
  ensureScriptSafe(sql); // S8
  h.db.exec(sql);
  return { ok: true };
}

function runTransaction(h: Handle, statements: Array<{ sql: string; params?: unknown }>) {
  if (h.readonly) throw new Error(`db "${h.name}" is readonly`);
  const tx = h.db.transaction((items: typeof statements) => {
    const results: Database.RunResult[] = [];
    for (const it of items) {
      const stmt = h.db.prepare(it.sql);
      // C1: callRun handles named-bind objects correctly.
      results.push(callRun(stmt, it.params as BindParams));
    }
    return results;
  });
  const raw = tx(statements);
  return {
    count: raw.length,
    results: raw.map((r) => ({ changes: r.changes, last_insert_rowid: Number(r.lastInsertRowid) })),
  };
}

function getSchema(h: Handle, table?: string) {
  if (table) {
    // S2: switch to table-valued PRAGMA functions with bound parameters.
    const info = h.db.prepare("SELECT * FROM pragma_table_info(?)").all(table);
    const fks = h.db.prepare("SELECT * FROM pragma_foreign_key_list(?)").all(table);
    const idxs = h.db.prepare("SELECT * FROM pragma_index_list(?)").all(table);
    const create = h.db.prepare("SELECT sql FROM sqlite_master WHERE type IN ('table','view') AND name = ?").get(table) as { sql?: string } | undefined;
    return { table, columns: info, foreign_keys: fks, indexes: idxs, create_sql: create?.sql ?? null };
  }
  const objects = h.db.prepare("SELECT type, name, tbl_name, sql FROM sqlite_master WHERE type IN ('table','view','index','trigger') AND name NOT LIKE 'sqlite_%' ORDER BY type, name").all();
  return { objects };
}

function runDumpSql(h: Handle) {
  // U10: full schema export.
  // B3: sqlite_master.sql is NULL for auto-created indexes (PRIMARY KEY,
  // UNIQUE constraints, INTEGER PRIMARY KEY rowid mapping). Re-running the
  // emitted CREATE TABLE statements re-creates those auto-indexes implicitly,
  // so excluding NULL-sql rows here is correct. Documented in the action
  // description and README.
  const rows = h.db
    .prepare(`SELECT type, name, tbl_name, sql FROM sqlite_master WHERE sql IS NOT NULL
             UNION ALL
             SELECT type, name, tbl_name, sql FROM sqlite_temp_master WHERE sql IS NOT NULL
             ORDER BY CASE type WHEN 'table' THEN 1 WHEN 'index' THEN 2 WHEN 'view' THEN 3 WHEN 'trigger' THEN 4 ELSE 5 END, name`)
    .all() as Array<{ type: string; name: string; tbl_name: string | null; sql: string }>;
  const dump = rows.map((r) => `${r.sql};`).join("\n\n");
  return { count: rows.length, objects: rows, sql: dump };
}

function runPragma(h: Handle, pragma: string) {
  ensurePragmaAllowed(pragma); // S3 + S8
  const out = h.db.pragma(pragma);
  return { pragma, result: out };
}

function listOpen() {
  return {
    count: registry.size,
    open: Array.from(registry.values()).map((h) => {
      let file_size: number | null = null;
      try { file_size = statSync(h.path).size; } catch { /* deleted or inaccessible */ }
      return {
        name: h.name,
        path: h.path,
        readonly: h.readonly,
        file_size,
        opened_at_ms_ago: Date.now() - h.opened_at,
      };
    }),
  };
}

function runExplain(h: Handle, sql: string, params?: unknown[] | Record<string, unknown>) {
  const wrapped = `EXPLAIN QUERY PLAN ${sql}`;
  const stmt = h.db.prepare(wrapped);
  // C1: callAll handles named-bind objects correctly.
  const rows = callAll(stmt, params) as Array<{ id: number; parent: number; notused: number; detail: string }>;
  // U6: walk parent chain so nested subplans render at their true depth instead
  // of always being indented by one when parent != 0.
  const byId = new Map<number, { id: number; parent: number; detail: string }>();
  for (const r of rows) byId.set(r.id, r);
  const depthOf = (r: { parent: number }): number => {
    let d = 0;
    let cur: { parent: number } | undefined = r;
    const seen = new Set<number>();
    while (cur && cur.parent && !seen.has(cur.parent)) {
      seen.add(cur.parent);
      const p = byId.get(cur.parent);
      if (!p) break;
      d++;
      cur = p;
    }
    return d;
  };
  const lines = rows.map((r) => `${"  ".repeat(depthOf(r))}${r.detail}`);
  return { sql, plan: rows, tree: lines.join("\n") };
}

function runVacuum(h: Handle, intoPath?: string) {
  if (h.readonly) throw new Error(`db "${h.name}" is readonly`);
  if (intoPath !== undefined) {
    // S7: sanitize + root-confine the destination.
    const abs = resolveSafePath(intoPath, "vacuum.dest_path");
    // U2: SQLite limitation: VACUUM INTO does not accept ? bind parameters; path is
    // single-quote escaped after resolveSafePath validation.
    h.db.exec(`VACUUM INTO '${abs.replace(/'/g, "''")}'`);
    return { ok: true, into: abs };
  }
  h.db.exec("VACUUM");
  return { ok: true };
}

function runAttach(h: Handle, path: string, schemaName: string) {
  validateSchemaName(schemaName); // S9
  if (schemaName === "main" || schemaName === "temp") {
    throw new Error(`cannot attach as reserved schema "${schemaName}"`);
  }
  const abs = resolveSafePath(path, "attach.path"); // S1
  const stmt = h.db.prepare("SELECT name FROM pragma_database_list WHERE name = ?");
  if (stmt.get(schemaName)) throw new Error(`schema already attached: ${schemaName}`);
  // S2: ATTACH ... AS <ident> — schema name validated above; quote identifier safely.
  h.db.prepare(`ATTACH DATABASE ? AS ${quoteIdent(schemaName)}`).run(abs);
  return { attached: true, schema: schemaName, path: abs };
}

function runDetach(h: Handle, schemaName: string) {
  validateSchemaName(schemaName);
  if (schemaName === "main" || schemaName === "temp") {
    throw new Error(`cannot detach reserved schema "${schemaName}"`);
  }
  h.db.exec(`DETACH DATABASE ${quoteIdent(schemaName)}`); // S2
  return { detached: true, schema: schemaName };
}

function runListSchemas(h: Handle) {
  const rows = h.db.prepare("SELECT name, file FROM pragma_database_list").all() as Array<{ name: string; file: string }>;
  return { count: rows.length, databases: rows };
}

function inferColumnType(samples: unknown[]): "INTEGER" | "REAL" | "TEXT" {
  // B2: only treat values as REAL when they actually contain `.` or an exponent.
  // Plain digit-only strings stay INTEGER even if very long; "1e5" → REAL.
  let sawFloat = false;
  let sawNonEmpty = false;
  const intRe = /^-?\d+$/;
  const floatRe = /^-?\d+(\.\d+)?([eE][+-]?\d+)?$/;
  for (const s of samples) {
    if (s === null || s === undefined || s === "") continue;
    sawNonEmpty = true;
    const str = String(s);
    if (intRe.test(str)) {
      // pure integer
      continue;
    }
    if (floatRe.test(str)) {
      sawFloat = true;
      continue;
    }
    return "TEXT";
  }
  if (!sawNonEmpty) return "TEXT";
  return sawFloat ? "REAL" : "INTEGER";
}

function coerceCell(val: string, type: "INTEGER" | "REAL" | "TEXT"): string | number | null {
  if (val === "" || val === null || val === undefined) return null;
  if (type === "INTEGER") {
    const n = parseInt(val, 10);
    return Number.isFinite(n) ? n : val;
  }
  if (type === "REAL") {
    const n = parseFloat(val);
    return Number.isFinite(n) ? n : val;
  }
  return val;
}

function importCsv(h: Handle, p: {
  path: string;
  table: string;
  delimiter?: string;
  has_header?: boolean;
  create_table?: boolean;
  replace_table?: boolean;
  columns?: string[];
  batch_size?: number;
  null_tokens?: string[];
}) {
  if (h.readonly) throw new Error(`db "${h.name}" is readonly`);
  const abs = resolveSafePath(p.path, "import_csv.path"); // S1
  // C3: detect UTF-16 BOM so CSV files exported from Excel/Windows aren't read
  // as garbage UTF-8. Sniff the first two bytes; transcode to UTF-8 if needed.
  const buf = readFileSync(abs);
  let content: string;
  if (buf.length >= 2 && buf[0] === 0xff && buf[1] === 0xfe) {
    // UTF-16 LE BOM
    content = buf.slice(2).toString("utf16le");
  } else if (buf.length >= 2 && buf[0] === 0xfe && buf[1] === 0xff) {
    // UTF-16 BE BOM — node has no native utf16be decoder; swap byte pairs into LE.
    const body = buf.slice(2);
    const swapped = Buffer.alloc(body.length);
    for (let i = 0; i + 1 < body.length; i += 2) {
      swapped[i] = body[i + 1]!;
      swapped[i + 1] = body[i]!;
    }
    content = swapped.toString("utf16le");
  } else {
    content = buf.toString("utf8");
  }
  const records = parseCsv(content, {
    delimiter: p.delimiter ?? ",",
    columns: p.has_header !== false,
    skip_empty_lines: true,
    trim: false,
    bom: true,
  }) as unknown[];
  if (records.length === 0) return { imported: 0, table: p.table };

  // S2: validate table identifier upfront — quoteIdent throws on bad names.
  const tableIdent = quoteIdent(p.table);

  let columns: string[];
  let rows: unknown[][];
  if (p.has_header !== false) {
    const firstRow = records[0] as Record<string, unknown> | undefined;
    columns = p.columns ?? (firstRow ? Object.keys(firstRow) : []);
    rows = (records as Record<string, unknown>[]).map((r) => columns.map((c) => r[c]));
  } else {
    const firstRow = records[0] as unknown[] | undefined;
    const width = firstRow ? firstRow.length : 0;
    columns = p.columns ?? Array.from({ length: width }, (_, i) => `col${i + 1}`);
    rows = records as unknown[][];
  }

  // S2: each column name validated by quoteIdent.
  const colIdents = columns.map(quoteIdent);

  const nullTokens = new Set([...(p.null_tokens ?? ["", "NULL", "null", "\\N"])]);
  const types: ("INTEGER" | "REAL" | "TEXT")[] = columns.map((_, ci) => {
    const sample = rows.slice(0, Math.min(rows.length, 200)).map((r) => r[ci]);
    return inferColumnType(sample.filter((v) => !(v == null || nullTokens.has(String(v)))));
  });

  // B5: wrap the entire import — DROP / CREATE / INSERT — in one transaction so a
  // mid-stream error rolls back the schema change too.
  // U1: batch_size, when supplied, rebuilds the prepared INSERT statement to
  // bind `batch_size` rows at a time (multi-VALUES insert). Whole import is
  // still atomic because of the outer transaction.
  // B4: tableExistsQuery is now prepared inside the transaction closure; safe
  // single-threaded since better-sqlite3 serializes calls on one handle.
  const batchSize = Math.max(1, Math.min(p.batch_size ?? 1, 1000));
  let total = 0;
  const importAll = h.db.transaction(() => {
    const tableExistsQuery = h.db.prepare(
      "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
    );
    if (p.replace_table && tableExistsQuery.get(p.table)) {
      h.db.exec(`DROP TABLE ${tableIdent}`);
    }
    const existsAfter = tableExistsQuery.get(p.table) !== undefined;
    if ((p.create_table !== false) && !existsAfter) {
      const cols = columns.map((_, i) => `${colIdents[i]} ${types[i]}`).join(", ");
      h.db.exec(`CREATE TABLE ${tableIdent} (${cols})`);
    }
    const placeholderRow = `(${columns.map(() => "?").join(",")})`;
    const singleStmt = h.db.prepare(
      `INSERT INTO ${tableIdent} (${colIdents.join(",")}) VALUES ${placeholderRow}`,
    );
    const coerceRow = (row: unknown[]): unknown[] =>
      row.map((v, i) => {
        if (v == null || nullTokens.has(String(v))) return null;
        const t = types[i] ?? "TEXT";
        return coerceCell(String(v), t);
      });

    if (batchSize <= 1) {
      for (const row of rows) {
        singleStmt.run(...coerceRow(row));
        total++;
      }
      return;
    }
    // multi-VALUES batched insert
    const batchStmt = h.db.prepare(
      `INSERT INTO ${tableIdent} (${colIdents.join(",")}) VALUES ${Array(batchSize).fill(placeholderRow).join(",")}`,
    );
    let i = 0;
    while (i + batchSize <= rows.length) {
      const flat: unknown[] = [];
      for (let j = 0; j < batchSize; j++) flat.push(...coerceRow(rows[i + j] as unknown[]));
      batchStmt.run(...flat);
      total += batchSize;
      i += batchSize;
    }
    for (; i < rows.length; i++) {
      singleStmt.run(...coerceRow(rows[i] as unknown[]));
      total++;
    }
  });
  importAll();

  return {
    imported: total,
    table: p.table,
    columns: columns.map((name, i) => ({ name, type: types[i] })),
    source: abs,
  };
}

function exportCsv(h: Handle, p: {
  sql: string;
  params?: unknown[] | Record<string, unknown>;
  output_path: string;
  delimiter?: string;
  header?: boolean;
}) {
  const stmt = h.db.prepare(p.sql);
  const abs = resolveSafePath(p.output_path, "export_csv.output_path"); // S1
  const cols = stmt.reader ? stmt.columns().map((c) => c.name) : [];
  // S4: iterate instead of materialising full result before applying MAX_LIMIT.
  const iter = callIterate(stmt, p.params) as IterableIterator<Record<string, unknown>>;
  const rows: Record<string, unknown>[] = [];
  let truncated = false;
  try {
    for (const row of iter) {
      if (rows.length >= MAX_LIMIT) { truncated = true; break; }
      rows.push(row as Record<string, unknown>);
    }
  } finally {
    try { iter.return?.(); } catch {}
  }
  // B8: encode BLOB cells as a string so csv-stringify produces a usable line.
  const data = rows.map((r) => cols.map((c) => blobToCsvCell(r[c])));
  const csv = stringifyCsv(data, {
    delimiter: p.delimiter ?? ",",
    header: p.header !== false,
    columns: p.header !== false ? cols : undefined,
  });
  const ws = createWriteStream(abs);
  return new Promise<{ rows: number; columns: string[]; path: string; bytes: number; truncated: boolean }>((res, rej) => {
    ws.on("error", rej);
    ws.on("finish", () => res({ rows: rows.length, columns: cols, path: abs, bytes: Buffer.byteLength(csv, "utf8"), truncated }));
    ws.end(csv);
  });
}

async function exportJson(h: Handle, p: {
  sql: string;
  params?: unknown[] | Record<string, unknown>;
  output_path: string;
  pretty?: boolean;
  ndjson?: boolean;
}) {
  const stmt = h.db.prepare(p.sql);
  const abs = resolveSafePath(p.output_path, "export_json.output_path"); // S1

  if (p.ndjson) {
    // B6: stream NDJSON straight to disk; never materialize all rows.
    // Drain via the write callback ONLY — no `once("drain")` parallel path,
    // which previously could double-resolve when both the callback and the
    // drain event fired.
    // B5: enforce MAX_LIMIT cap and surface `truncated`.
    // B8: serializeRow encodes Buffers as { $blob: <base64> }.
    const ws = createWriteStream(abs);
    let count = 0;
    let bytes = 0;
    let truncated = false;
    const drain = (chunk: string): Promise<void> =>
      new Promise<void>((res, rej) => {
        ws.write(chunk, "utf8", (err) => {
          if (err) rej(err); else res();
        });
      });
    try {
      // C1: callIterate handles named-bind objects correctly.
      const iter = callIterate(stmt, p.params);
      for (const row of iter) {
        if (count >= MAX_LIMIT) { truncated = true; break; }
        const line = JSON.stringify(serializeRow(row)) + "\n";
        bytes += Buffer.byteLength(line, "utf8");
        await drain(line);
        count++;
      }
    } catch (e) {
      ws.destroy();
      throw e;
    }
    await new Promise<void>((res, rej) => {
      ws.on("error", rej);
      ws.on("finish", () => res());
      ws.end();
    });
    return { rows: count, path: abs, bytes, format: "ndjson" as const, truncated };
  }

  // JSON array (regular or pretty) — must materialize. Apply MAX_LIMIT.
  // C1: callAll handles named-bind objects correctly.
  const allRows = callAll(stmt, p.params);
  const truncated = allRows.length > MAX_LIMIT;
  const rows = (truncated ? allRows.slice(0, MAX_LIMIT) : allRows).map(serializeRow);
  const content = p.pretty ? JSON.stringify(rows, null, 2) : JSON.stringify(rows);
  await new Promise<void>((res, rej) => {
    const ws = createWriteStream(abs);
    ws.on("error", rej);
    ws.on("finish", () => res());
    ws.end(content);
  });
  return { rows: rows.length, path: abs, bytes: Buffer.byteLength(content, "utf8"), format: "json" as const, truncated };
}

function backup(h: Handle, destPath: string) {
  const abs = resolveSafePath(destPath, "backup.dest_path"); // S1
  return h.db.backup(abs).then((progress) => ({
    path: abs,
    total_pages: progress.totalPages,
    remaining_pages: progress.remainingPages,
  }));
}

function textContent(data: unknown) {
  const text = typeof data === "string" ? data : JSON.stringify(data, null, 2);
  return { content: [{ type: "text" as const, text }] };
}

function errContent(msg: string) {
  return { content: [{ type: "text" as const, text: msg }], isError: true };
}

// B13: include err.code (SQLite SQLITE_* codes) for richer LLM debugging.
function formatError(err: unknown): string {
  if (err instanceof Error) {
    const code = (err as NodeJS.ErrnoException).code ? ` [${(err as NodeJS.ErrnoException).code}]` : "";
    return `${err.name}${code}: ${err.message}`;
  }
  return `Error: ${String(err)}`;
}

const server = new McpServer({ name: "sqlite", version: PKG_VERSION });

server.tool(
  "sqlite",
  `SQLite access via better-sqlite3 (synchronous, prebuilt binaries).

Connection model:
- Multiple DBs can be open concurrently, each identified by 'name' (default "default").
- Actions implicitly use name="default" when name is omitted.
- Close DBs with action=close to free resources.

Actions:
- open: open/create a DB at 'path'. Options: readonly (default false), create (default true), pragmas[] (applied on open, e.g. ["journal_mode = WAL", "foreign_keys = ON"]).
- close: close the named DB.
- list_open: enumerate all open DBs.
- query: prepared SELECT (returns rows + column metadata). Rows truncated at 'limit' (default SQLITE_MAX_ROWS=1000).
- execute: single prepared DML/DDL statement (INSERT/UPDATE/DELETE/CREATE ...). Returns changes + last_insert_rowid.
- execute_script: multi-statement script (CREATE TABLE ...; INSERT ...; ...). No params, no row returns.
- transaction: run multiple prepared statements atomically.
- schema: list all objects (tables/views/indexes/triggers). Pass 'table' to get columns/FKs/indexes for a specific table.
- pragma: run a PRAGMA query. e.g. "journal_mode", "foreign_keys", "table_list".
- backup: online backup to 'dest_path'.
- dump_sql: return CREATE / CREATE INDEX / etc. for full schema export.

params may be an array (positional ?) or object (named @name / :name / $name).`,
  {
    action: z.enum([
      "open", "close", "list_open", "list_schemas",
      "query", "execute", "execute_script", "transaction",
      "schema", "pragma", "backup",
      "explain", "vacuum", "attach", "detach",
      "import_csv", "export_csv", "export_json",
      "dump_sql",
    ]).describe("Action"),
    name: z.string().optional().describe("DB handle name (default 'default')"),
    path: z.string().optional().describe("open/import_csv: file path"),
    readonly: z.boolean().optional().describe("open: open as readonly"),
    create: z.boolean().optional().describe("open: allow create if missing (default true)"),
    pragmas: z.union([z.array(z.string()), z.string()]).optional().describe("open: PRAGMAs to apply on open"),
    sql: z.string().optional().describe("query/execute/execute_script/explain/export_*: SQL text"),
    params: z.any().optional().describe("query/execute/explain/export_*: positional array or named object"),
    limit: z.number().int().positive().optional().describe("query: row cap (default 1000)"),
    table: z.string().optional().describe("schema/import_csv: table name"),
    pragma: z.string().optional().describe("pragma: pragma expression"),
    statements: z.union([z.array(z.object({ sql: z.string(), params: z.any().optional() })), z.string()]).optional().describe("transaction: list of prepared statements"),
    dest_path: z.string().optional().describe("backup/vacuum(INTO): destination path"),
    schema_name: z.string().optional().describe("attach/detach: schema alias"),
    // csv i/o
    delimiter: z.string().optional().describe("import_csv/export_csv: field delimiter (default ',')"),
    has_header: z.boolean().optional().describe("import_csv: first row is header (default true)"),
    header: z.boolean().optional().describe("export_csv: write header row (default true)"),
    create_table: z.boolean().optional().describe("import_csv: auto-CREATE if missing (default true)"),
    replace_table: z.boolean().optional().describe("import_csv: DROP + recreate if exists"),
    columns: z.union([z.array(z.string()), z.string()]).optional().describe("import_csv: explicit column names"),
    batch_size: z.number().int().positive().optional().describe("import_csv: rows per transaction (default 1000)"),
    null_tokens: z.union([z.array(z.string()), z.string()]).optional().describe("import_csv: strings treated as NULL (default ['', 'NULL', 'null', '\\\\N'])"),
    output_path: z.string().optional().describe("export_csv/export_json: destination path"),
    pretty: z.boolean().optional().describe("export_json: pretty-print"),
    ndjson: z.boolean().optional().describe("export_json: one-object-per-line"),
  },
  async (p) => {
    try {
      const params = coerceObject<unknown[] | Record<string, unknown>>(p.params);
      const pragmas = coerceObject<string[]>(p.pragmas);
      const statements = coerceObject<Array<{ sql: string; params?: unknown }>>(p.statements);
      const columns = coerceObject<string[]>(p.columns);
      const nullTokens = coerceObject<string[]>(p.null_tokens);
      switch (p.action) {
        case "open": {
          if (!p.path) return errContent("open requires 'path'");
          const h = openDb({ path: p.path, name: p.name, readonly: p.readonly, create: p.create, pragmas });
          return textContent({ opened: true, name: h.name, path: h.path, readonly: h.readonly });
        }
        case "close":
          return textContent(closeDb(p.name));
        case "list_open":
          return textContent(listOpen());
        case "query": {
          if (!p.sql) return errContent("query requires 'sql'");
          return textContent(runQuery(getHandle(p.name), p.sql, params, p.limit));
        }
        case "execute": {
          if (!p.sql) return errContent("execute requires 'sql'");
          return textContent(runExec(getHandle(p.name), p.sql, params));
        }
        case "execute_script": {
          if (!p.sql) return errContent("execute_script requires 'sql'");
          return textContent(runExecScript(getHandle(p.name), p.sql));
        }
        case "transaction": {
          if (!statements) return errContent("transaction requires 'statements'");
          return textContent(runTransaction(getHandle(p.name), statements.map((s) => ({
            sql: s.sql,
            params: coerceObject<unknown[] | Record<string, unknown>>(s.params),
          }))));
        }
        case "schema":
          return textContent(getSchema(getHandle(p.name), p.table));
        case "pragma": {
          if (!p.pragma) return errContent("pragma requires 'pragma'");
          return textContent(runPragma(getHandle(p.name), p.pragma));
        }
        case "backup": {
          if (!p.dest_path) return errContent("backup requires 'dest_path'");
          // B7: track async backup for clean shutdown.
          return textContent(await trackAsync(() => backup(getHandle(p.name), p.dest_path!)));
        }
        case "explain": {
          if (!p.sql) return errContent("explain requires 'sql'");
          return textContent(runExplain(getHandle(p.name), p.sql, params));
        }
        case "vacuum":
          return textContent(runVacuum(getHandle(p.name), p.dest_path));
        case "attach": {
          if (!p.path || !p.schema_name) return errContent("attach requires 'path' and 'schema_name'");
          return textContent(runAttach(getHandle(p.name), p.path, p.schema_name));
        }
        case "detach": {
          if (!p.schema_name) return errContent("detach requires 'schema_name'");
          return textContent(runDetach(getHandle(p.name), p.schema_name));
        }
        case "list_schemas":
          return textContent(runListSchemas(getHandle(p.name)));
        case "import_csv": {
          if (!p.path || !p.table) return errContent("import_csv requires 'path' and 'table'");
          return textContent(importCsv(getHandle(p.name), {
            path: p.path, table: p.table,
            delimiter: p.delimiter, has_header: p.has_header,
            create_table: p.create_table, replace_table: p.replace_table,
            columns, batch_size: p.batch_size,
            null_tokens: nullTokens,
          }));
        }
        case "export_csv": {
          if (!p.sql || !p.output_path) return errContent("export_csv requires 'sql' and 'output_path'");
          // B7: track async export for clean shutdown.
          return textContent(await trackAsync(() => exportCsv(getHandle(p.name), {
            sql: p.sql!, params, output_path: p.output_path!,
            delimiter: p.delimiter, header: p.header,
          })));
        }
        case "export_json": {
          if (!p.sql || !p.output_path) return errContent("export_json requires 'sql' and 'output_path'");
          // B7: track async export for clean shutdown.
          return textContent(await trackAsync(() => exportJson(getHandle(p.name), {
            sql: p.sql!, params, output_path: p.output_path!,
            pretty: p.pretty, ndjson: p.ndjson,
          })));
        }
        case "dump_sql":
          return textContent(runDumpSql(getHandle(p.name)));
      }
    } catch (err) {
      return errContent(formatError(err));
    }
    return errContent("unreachable");
  },
);

// B12: shutdown handlers — close every open DB.
// B7: wait for in-flight async ops (export / backup) to drain before close.
//   - poll every 25ms up to 3000ms; then close regardless.
let shuttingDown = false;
let activeTransport: StdioServerTransport | undefined;

async function shutdown(signal: NodeJS.Signals) {
  if (shuttingDown) return;
  shuttingDown = true;
  // B5: stop accepting new requests before closing DB handles.
  try { await activeTransport?.close(); } catch {}
  const deadline = Date.now() + 3000;
  while (inflightAsync > 0 && Date.now() < deadline) {
    await new Promise<void>((res) => setTimeout(res, 25));
  }
  closeAllDbs();
  // Give the event loop a tick to flush stderr.
  setImmediate(() => process.exit(signal === "SIGINT" ? 130 : 143));
}
process.on("SIGINT", () => { void shutdown("SIGINT"); });
process.on("SIGTERM", () => { void shutdown("SIGTERM"); });

async function main() {
  const transport = new StdioServerTransport();
  activeTransport = transport;
  await server.connect(transport);
}

main().catch((err) => { console.error("Fatal:", err); process.exit(1); });
