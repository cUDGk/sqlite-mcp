#!/usr/bin/env node
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import Database from "better-sqlite3";
import { existsSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
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
const MAX_ROWS = parseInt(process.env.SQLITE_MAX_ROWS ?? "1000", 10);

// Some LLM tool-call paths deliver object/array arguments as JSON strings.
// Parse those back into structured values before use.
function coerceObject<T>(v: unknown): T | undefined {
  if (v === undefined || v === null) return undefined;
  if (typeof v === "string") {
    const s = v.trim();
    if (s === "") return undefined;
    try {
      return JSON.parse(s) as T;
    } catch (e: any) {
      throw new Error(`failed to parse JSON string argument: ${e.message}`);
    }
  }
  return v as T;
}

function getHandle(name?: string): Handle {
  const n = name ?? DEFAULT_NAME;
  const h = registry.get(n);
  if (!h) throw new Error(`no db open for name="${n}". Call action=open first.`);
  return h;
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
  const abs = resolve(p.path);
  const readonly = p.readonly === true;
  const exists = existsSync(abs);
  if (!exists && p.create === false) {
    throw new Error(`file not found: ${abs} (create=false)`);
  }
  const db = new Database(abs, { readonly, fileMustExist: p.create === false });
  for (const pr of p.pragmas ?? []) {
    try { db.pragma(pr); } catch (e: any) {
      throw new Error(`invalid pragma "${pr}": ${e.message}`);
    }
  }
  const h: Handle = { name: n, path: abs, db, readonly, opened_at: Date.now() };
  registry.set(n, h);
  return h;
}

function closeDb(name?: string) {
  const n = name ?? DEFAULT_NAME;
  const h = registry.get(n);
  if (!h) return { closed: false, name: n, reason: "not open" };
  try { h.db.close(); } catch {}
  registry.delete(n);
  return { closed: true, name: n };
}

function runQuery(h: Handle, sql: string, params?: unknown[] | Record<string, unknown>, limit?: number) {
  const stmt = h.db.prepare(sql);
  const lim = limit ?? MAX_ROWS;
  const rows = params !== undefined ? stmt.all(params as any) : stmt.all();
  const truncated = rows.length > lim;
  const kept = truncated ? rows.slice(0, lim) : rows;
  return {
    row_count: rows.length,
    rows: kept,
    truncated,
    limit: lim,
    columns: stmt.columns().map((c) => ({ name: c.name, type: c.type, column: c.column, table: c.table })),
  };
}

function runExec(h: Handle, sql: string, params?: unknown[] | Record<string, unknown>) {
  if (h.readonly) throw new Error(`db "${h.name}" is readonly`);
  const stmt = h.db.prepare(sql);
  const info = params !== undefined ? stmt.run(params as any) : stmt.run();
  return {
    changes: info.changes,
    last_insert_rowid: Number(info.lastInsertRowid),
  };
}

function runExecScript(h: Handle, sql: string) {
  if (h.readonly) throw new Error(`db "${h.name}" is readonly`);
  h.db.exec(sql);
  return { ok: true };
}

function runTransaction(h: Handle, statements: Array<{ sql: string; params?: unknown }>) {
  if (h.readonly) throw new Error(`db "${h.name}" is readonly`);
  const tx = h.db.transaction((items: typeof statements) => {
    const results: any[] = [];
    for (const it of items) {
      const stmt = h.db.prepare(it.sql);
      if (it.params !== undefined) {
        results.push(stmt.run(it.params as any));
      } else {
        results.push(stmt.run());
      }
    }
    return results;
  });
  const raw = tx(statements);
  return {
    count: raw.length,
    results: raw.map((r: any) => ({ changes: r.changes, last_insert_rowid: Number(r.lastInsertRowid) })),
  };
}

function getSchema(h: Handle, table?: string) {
  if (table) {
    const info = h.db.prepare(`PRAGMA table_info(${JSON.stringify(table)})`).all();
    const fks = h.db.prepare(`PRAGMA foreign_key_list(${JSON.stringify(table)})`).all();
    const idxs = h.db.prepare(`PRAGMA index_list(${JSON.stringify(table)})`).all();
    const create = h.db.prepare("SELECT sql FROM sqlite_master WHERE type IN ('table','view') AND name = ?").get(table) as { sql?: string } | undefined;
    return { table, columns: info, foreign_keys: fks, indexes: idxs, create_sql: create?.sql ?? null };
  }
  const objects = h.db.prepare("SELECT type, name, tbl_name, sql FROM sqlite_master WHERE type IN ('table','view','index','trigger') AND name NOT LIKE 'sqlite_%' ORDER BY type, name").all();
  return { objects };
}

function runPragma(h: Handle, pragma: string) {
  const out = h.db.pragma(pragma);
  return { pragma, result: out };
}

function listOpen() {
  return {
    count: registry.size,
    open: Array.from(registry.values()).map((h) => ({
      name: h.name,
      path: h.path,
      readonly: h.readonly,
      file_size: existsSync(h.path) ? statSync(h.path).size : null,
      opened_at_ms_ago: Date.now() - h.opened_at,
    })),
  };
}

function runExplain(h: Handle, sql: string, params?: unknown[] | Record<string, unknown>) {
  const wrapped = `EXPLAIN QUERY PLAN ${sql}`;
  const stmt = h.db.prepare(wrapped);
  const rows = (params !== undefined ? stmt.all(params as any) : stmt.all()) as Array<{ id: number; parent: number; notused: number; detail: string }>;
  const lines = rows.map((r) => {
    const indent = "  ".repeat(Math.max(0, r.parent ? 1 : 0));
    return `${indent}${r.detail}`;
  });
  return { sql, plan: rows, tree: lines.join("\n") };
}

function runVacuum(h: Handle, intoPath?: string) {
  if (h.readonly) throw new Error(`db "${h.name}" is readonly`);
  if (intoPath) {
    const abs = resolve(intoPath);
    h.db.exec(`VACUUM INTO '${abs.replace(/'/g, "''")}'`);
    return { ok: true, into: abs };
  }
  h.db.exec("VACUUM");
  return { ok: true };
}

function runAttach(h: Handle, path: string, schemaName: string) {
  if (schemaName === "main" || schemaName === "temp") throw new Error(`cannot attach as reserved schema "${schemaName}"`);
  const abs = resolve(path);
  const stmt = h.db.prepare("SELECT name FROM pragma_database_list WHERE name = ?");
  if (stmt.get(schemaName)) throw new Error(`schema already attached: ${schemaName}`);
  h.db.prepare(`ATTACH DATABASE ? AS ${JSON.stringify(schemaName)}`).run(abs);
  return { attached: true, schema: schemaName, path: abs };
}

function runDetach(h: Handle, schemaName: string) {
  if (schemaName === "main" || schemaName === "temp") throw new Error(`cannot detach reserved schema "${schemaName}"`);
  h.db.exec(`DETACH DATABASE ${JSON.stringify(schemaName)}`);
  return { detached: true, schema: schemaName };
}

function runListSchemas(h: Handle) {
  const rows = h.db.prepare("SELECT name, file FROM pragma_database_list").all() as Array<{ name: string; file: string }>;
  return { count: rows.length, databases: rows };
}

function inferColumnType(samples: unknown[]): "INTEGER" | "REAL" | "TEXT" {
  let sawNumber = false;
  let sawFloat = false;
  let sawNonEmpty = false;
  for (const s of samples) {
    if (s === null || s === undefined || s === "") continue;
    sawNonEmpty = true;
    const str = String(s);
    if (/^-?\d+$/.test(str)) {
      sawNumber = true;
    } else if (/^-?\d+(\.\d+)?([eE][+-]?\d+)?$/.test(str)) {
      sawNumber = true;
      sawFloat = true;
    } else {
      return "TEXT";
    }
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
  const abs = resolve(p.path);
  if (!existsSync(abs)) throw new Error(`csv not found: ${abs}`);
  const content = readFileSync(abs, "utf8");
  const records = parseCsv(content, {
    delimiter: p.delimiter ?? ",",
    columns: p.has_header !== false,
    skip_empty_lines: true,
    trim: false,
    bom: true,
  }) as any[];
  if (records.length === 0) return { imported: 0, table: p.table };

  let columns: string[];
  let rows: unknown[][];
  if (p.has_header !== false) {
    columns = p.columns ?? Object.keys(records[0]);
    rows = records.map((r: any) => columns.map((c) => r[c]));
  } else {
    const width = (records[0] as any[]).length;
    columns = p.columns ?? Array.from({ length: width }, (_, i) => `col${i + 1}`);
    rows = records as any[][];
  }

  const tableExistsQuery = h.db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?");
  if (p.replace_table && tableExistsQuery.get(p.table)) {
    h.db.exec(`DROP TABLE ${JSON.stringify(p.table)}`);
  }

  const nullTokens = new Set([...(p.null_tokens ?? ["", "NULL", "null", "\\N"])]);
  const types: ("INTEGER" | "REAL" | "TEXT")[] = columns.map((_, ci) => {
    const sample = rows.slice(0, Math.min(rows.length, 200)).map((r) => r[ci]);
    return inferColumnType(sample.filter((v) => !(v == null || nullTokens.has(String(v)))));
  });

  const existsAfter = tableExistsQuery.get(p.table) !== undefined;
  if ((p.create_table !== false) && !existsAfter) {
    const cols = columns.map((c, i) => `${JSON.stringify(c)} ${types[i]}`).join(", ");
    h.db.exec(`CREATE TABLE ${JSON.stringify(p.table)} (${cols})`);
  }

  const stmt = h.db.prepare(`INSERT INTO ${JSON.stringify(p.table)} (${columns.map((c) => JSON.stringify(c)).join(",")}) VALUES (${columns.map(() => "?").join(",")})`);
  const tx = h.db.transaction((batch: unknown[][]) => {
    for (const row of batch) {
      const values = row.map((v, i) => {
        if (v == null || nullTokens.has(String(v))) return null;
        return coerceCell(String(v), types[i]);
      });
      stmt.run(values);
    }
  });

  const batchSize = p.batch_size ?? 1000;
  let total = 0;
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    tx(batch);
    total += batch.length;
  }
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
  const rows = (p.params !== undefined ? stmt.all(p.params as any) : stmt.all()) as any[];
  const abs = resolve(p.output_path);
  const cols = stmt.columns().map((c) => c.name);
  const data = rows.map((r) => cols.map((c) => r[c]));
  const csv = stringifyCsv(data, {
    delimiter: p.delimiter ?? ",",
    header: p.header !== false,
    columns: p.header !== false ? cols : undefined,
  });
  writeFileSync(abs, csv, "utf8");
  return { rows: rows.length, columns: cols, path: abs, bytes: Buffer.byteLength(csv, "utf8") };
}

function exportJson(h: Handle, p: {
  sql: string;
  params?: unknown[] | Record<string, unknown>;
  output_path: string;
  pretty?: boolean;
  ndjson?: boolean;
}) {
  const stmt = h.db.prepare(p.sql);
  const rows = (p.params !== undefined ? stmt.all(p.params as any) : stmt.all()) as any[];
  const abs = resolve(p.output_path);
  let content: string;
  if (p.ndjson) {
    content = rows.map((r) => JSON.stringify(r)).join("\n") + "\n";
  } else {
    content = p.pretty ? JSON.stringify(rows, null, 2) : JSON.stringify(rows);
  }
  writeFileSync(abs, content, "utf8");
  return { rows: rows.length, path: abs, bytes: Buffer.byteLength(content, "utf8"), format: p.ndjson ? "ndjson" : "json" };
}

function backup(h: Handle, destPath: string) {
  const abs = resolve(destPath);
  return h.db.backup(abs).then((progress: any) => ({
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

const server = new McpServer({ name: "sqlite", version: "0.1.0" });

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

params may be an array (positional ?) or object (named @name / :name / $name).`,
  {
    action: z.enum([
      "open", "close", "list_open", "list_schemas",
      "query", "execute", "execute_script", "transaction",
      "schema", "pragma", "backup",
      "explain", "vacuum", "attach", "detach",
      "import_csv", "export_csv", "export_json",
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
          return textContent(await backup(getHandle(p.name), p.dest_path));
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
          return textContent(exportCsv(getHandle(p.name), {
            sql: p.sql, params, output_path: p.output_path,
            delimiter: p.delimiter, header: p.header,
          }));
        }
        case "export_json": {
          if (!p.sql || !p.output_path) return errContent("export_json requires 'sql' and 'output_path'");
          return textContent(exportJson(getHandle(p.name), {
            sql: p.sql, params, output_path: p.output_path,
            pretty: p.pretty, ndjson: p.ndjson,
          }));
        }
      }
    } catch (err: any) {
      return errContent(`${err?.name ?? "Error"}: ${err?.message ?? String(err)}`);
    }
    return errContent("unreachable");
  },
);

process.on("SIGINT", () => { for (const h of registry.values()) try { h.db.close(); } catch {} ; process.exit(0); });
process.on("SIGTERM", () => { for (const h of registry.values()) try { h.db.close(); } catch {} ; process.exit(0); });

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch((err) => { console.error("Fatal:", err); process.exit(1); });
