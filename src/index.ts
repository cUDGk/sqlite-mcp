#!/usr/bin/env node
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import Database from "better-sqlite3";
import { existsSync, statSync } from "node:fs";
import { resolve } from "node:path";

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
      "open", "close", "list_open",
      "query", "execute", "execute_script", "transaction",
      "schema", "pragma", "backup",
    ]).describe("Action"),
    name: z.string().optional().describe("DB handle name (default 'default')"),
    path: z.string().optional().describe("open: DB file path. backup is unrelated."),
    readonly: z.boolean().optional().describe("open: open as readonly"),
    create: z.boolean().optional().describe("open: allow create if missing (default true)"),
    pragmas: z.array(z.string()).optional().describe("open: PRAGMAs to apply on open"),
    sql: z.string().optional().describe("query/execute/execute_script: SQL text"),
    params: z.any().optional().describe("query/execute: positional array or named object"),
    limit: z.number().int().positive().optional().describe("query: row cap (default 1000)"),
    table: z.string().optional().describe("schema: specific table name"),
    pragma: z.string().optional().describe("pragma: pragma expression, e.g. 'journal_mode = WAL' or 'foreign_keys'"),
    statements: z.array(z.object({ sql: z.string(), params: z.any().optional() })).optional().describe("transaction: list of prepared statements"),
    dest_path: z.string().optional().describe("backup: destination path"),
  },
  async (p) => {
    try {
      switch (p.action) {
        case "open": {
          if (!p.path) return errContent("open requires 'path'");
          const h = openDb({ path: p.path, name: p.name, readonly: p.readonly, create: p.create, pragmas: p.pragmas });
          return textContent({ opened: true, name: h.name, path: h.path, readonly: h.readonly });
        }
        case "close":
          return textContent(closeDb(p.name));
        case "list_open":
          return textContent(listOpen());
        case "query": {
          if (!p.sql) return errContent("query requires 'sql'");
          return textContent(runQuery(getHandle(p.name), p.sql, p.params, p.limit));
        }
        case "execute": {
          if (!p.sql) return errContent("execute requires 'sql'");
          return textContent(runExec(getHandle(p.name), p.sql, p.params));
        }
        case "execute_script": {
          if (!p.sql) return errContent("execute_script requires 'sql'");
          return textContent(runExecScript(getHandle(p.name), p.sql));
        }
        case "transaction": {
          if (!p.statements) return errContent("transaction requires 'statements'");
          return textContent(runTransaction(getHandle(p.name), p.statements));
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
