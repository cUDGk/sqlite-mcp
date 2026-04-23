<div align="center">

# sqlite-mcp

### SQLite を LLM から叩く MCP サーバー

[![TypeScript](https://img.shields.io/badge/TypeScript-5.7-3178C6?style=flat&logo=typescript&logoColor=white)](src/index.ts)
[![Node.js](https://img.shields.io/badge/Node.js-%E2%89%A520-339933?style=flat&logo=node.js&logoColor=white)](package.json)
[![SQLite](https://img.shields.io/badge/SQLite-3-003B57?style=flat&logo=sqlite&logoColor=white)](https://www.sqlite.org/)
[![MCP](https://img.shields.io/badge/MCP-stdio-6E56CF?style=flat)](https://modelcontextprotocol.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green?style=flat)](LICENSE)

**better-sqlite3 (同期・prebuilt) で複数 DB 同時接続、prepared statement、トランザクション、スキーマ検査。**

---

</div>

## 概要

`better-sqlite3` で複数 DB を名前付きハンドルとして保持し、LLM からは**宣言的アクション**で SELECT / DML / DDL / トランザクション / PRAGMA / スキーマ検査を叩ける。行数は 1000 で自動 truncate、上書き可能。

## 特徴

| アクション | 用途 |
|---|---|
| `open` / `close` / `list_open` | DB ライフサイクル。`readonly` / `create` / `pragmas[]` 指定可 |
| `query` | prepared SELECT。`params` は配列（`?`）またはオブジェクト（`@name` / `:name` / `$name`）。行数 + カラム型情報 |
| `execute` | 単一 prepared DML/DDL。`changes` / `last_insert_rowid` を返す |
| `execute_script` | 複数文スクリプト（`;` 区切り）。params 不可、戻り値なし |
| `transaction` | `statements[]` を atomic に実行 |
| `schema` | 全オブジェクト列挙。`table` 指定で columns / FKs / indexes |
| `pragma` | `PRAGMA ...` 直叩き |
| `backup` | オンラインバックアップ |

## インストール

```bash
git clone https://github.com/cUDGk/sqlite-mcp.git
cd sqlite-mcp && npm install && npm run build
```

`better-sqlite3` は prebuilt バイナリが Node 20 Windows/macOS/Linux に配布されているので、通常は追加ツール不要。

## 使い方

```bash
claude mcp add sqlite -- node C:/Users/user/Desktop/sqlite-mcp/dist/index.js
```

### 環境変数

| 変数 | デフォルト | 用途 |
|---|---|---|
| `SQLITE_MAX_ROWS` | `1000` | `query` のデフォルト行数上限 |

### 呼び出し例

```json
{"action": "open", "path": "C:/tmp/notes.db", "pragmas": ["journal_mode = WAL", "foreign_keys = ON"]}

{"action": "execute_script", "sql": "CREATE TABLE notes (id INTEGER PRIMARY KEY, title TEXT NOT NULL, body TEXT, created_at INTEGER DEFAULT (unixepoch()));"}

{"action": "execute", "sql": "INSERT INTO notes (title, body) VALUES (?, ?)", "params": ["hello", "world"]}

{"action": "query", "sql": "SELECT id, title FROM notes WHERE title LIKE @q", "params": {"q": "%hello%"}}

{"action": "schema", "table": "notes"}
```

複数 DB 同時操作:

```json
{"action": "open", "name": "a", "path": "a.db"}
{"action": "open", "name": "b", "path": "b.db"}
{"action": "query", "name": "a", "sql": "SELECT * FROM foo"}
{"action": "transaction", "name": "b", "statements": [
  {"sql": "INSERT INTO t VALUES (?)", "params": [1]},
  {"sql": "INSERT INTO t VALUES (?)", "params": [2]}
]}
```

## 設計メモ

- **同期 API** — `better-sqlite3` は同期クライアントなので await なしで高速（MCP 経由でも 1 回の tool call で 1 往復）
- **`readonly: true` で安全化** — LLM に勝手に DDL/DML させたくない時に
- **prepared statement に強制** — `execute` は常に `.prepare()`、SQL インジェクション面で安全（文字列連結でクエリを作らせない）

## Attribution

- [better-sqlite3](https://github.com/WiseLibs/better-sqlite3) — 高速な同期 SQLite バインディング
- [SQLite](https://www.sqlite.org/)
- [Model Context Protocol](https://modelcontextprotocol.io/)

## ライセンス

MIT License © 2026 cUDGk — 詳細は [LICENSE](LICENSE) を参照。
