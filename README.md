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
| `open` / `close` / `list_open` | DB ライフサイクル。`readonly` / `create` / `pragmas[]` 指定可。`readonly: true` は対象ファイルが既に存在している必要がある（無ければ ENOENT エラー） |
| `query` | prepared SELECT。`params` は配列（`?`）またはオブジェクト（`@name` / `:name` / `$name`）。行数 + カラム型情報 |
| `execute` | 単一 prepared DML/DDL。`changes` / `last_insert_rowid` を返す |
| `execute_script` | 複数文スクリプト（`;` 区切り）。params 不可、`{ok: true}` を返す |
| `transaction` | `statements[]` を atomic に実行 |
| `schema` | 全オブジェクト列挙。`table` 指定で columns / FKs / indexes |
| `pragma` | `PRAGMA ...` 直叩き |
| `backup` | オンラインバックアップ |
| `explain` | `EXPLAIN QUERY PLAN` で実行計画を構造化 + tree 文字列で返却 |
| `vacuum` | `VACUUM`、`dest_path` 指定で `VACUUM INTO` に切替 |
| `attach` / `detach` | `ATTACH DATABASE ... AS <schema_name>` / `DETACH` のラッパー |
| `list_schemas` | `pragma_database_list` を返却 (`main` / `temp` / attached 一覧) |
| `import_csv` | CSV を指定テーブルにインポート。型推論 (INTEGER/REAL/TEXT)、`has_header` / `delimiter` / `null_tokens` (既定 `["", "NULL", "null", "\\N"]`) / `create_table` / `replace_table` / `batch_size` 対応。**全体を 1 トランザクションで実行**するので途中失敗時は完全ロールバック |
| `export_csv` / `export_json` | query 結果を `output_path` に書き出し。JSON は `pretty` / `ndjson` 切替（`ndjson` は `stmt.iterate()` でストリーム書き込み） |
| `dump_sql` | `sqlite_master` から `CREATE …` 文を全部集めてスキーマダンプを返す。**自動生成インデックス**（`PRIMARY KEY` / `UNIQUE` 等）は `sqlite_master.sql` が `NULL` のため除外されるが、`CREATE TABLE` 文を再実行すれば暗黙に再作成される |

## インストール

```bash
git clone https://github.com/cUDGk/sqlite-mcp.git
cd sqlite-mcp && npm install && npm run build
```

`better-sqlite3` は prebuilt バイナリが Node 20 Windows/macOS/Linux に配布されているので、通常は追加ツール不要。

## 使い方

```bash
claude mcp add sqlite -- node /path/to/sqlite-mcp/dist/index.js
```

### 環境変数

| 変数 | デフォルト | 用途 |
|---|---|---|
| `SQLITE_MAX_ROWS` | `1000` | `query` のデフォルト行数上限 |
| `SQLITE_MAX_LIMIT` | `100000` | `query` の `limit` 引数の上限 (これより大きい値はエラー) |
| `SQLITE_ALLOWED_ROOT` | サーバの CWD | `open` / `attach` / `import_csv` / `export_csv` / `export_json` / `vacuum INTO` / `backup` の対象パスを `path.resolve` + `path.relative` ベースで閉じ込める。文字列 startsWith ではないので `..` での脱出不可 |
| `SQLITE_ALLOW_DANGEROUS` | (未設定) | `1` を設定すると、PRAGMA allowlist 外の `pragma` 実行と、`execute_script` 内の `ATTACH DATABASE` / `PRAGMA writable_schema` の実行が許可される。ただし `writable_schema` / `temp_store_directory` / `data_store_directory` は常時ブロック |

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

CSV インポート (型自動推論):

```json
{"action": "import_csv",
 "path": "C:/data/users.csv",
 "table": "users",
 "replace_table": true}
```

query 結果を CSV に書き出し:

```json
{"action": "export_csv",
 "sql": "SELECT name, age FROM users WHERE city = ?",
 "params": ["Tokyo"],
 "output_path": "C:/tmp/tokyo.csv"}
```

NDJSON でストリーミング用に:

```json
{"action": "export_json",
 "sql": "SELECT * FROM events",
 "output_path": "C:/tmp/events.ndjson",
 "ndjson": true}
```

EXPLAIN QUERY PLAN で index 効いてるか確認:

```json
{"action": "explain",
 "sql": "SELECT * FROM orders WHERE user_id = ? AND status = ?",
 "params": [42, "paid"]}
```

別 DB を attach して cross-db join:

```json
{"action": "attach", "path": "analytics.db", "schema_name": "a"}
{"action": "query", "sql": "SELECT u.name, COUNT(*) FROM users u JOIN a.events e ON e.user_id = u.id GROUP BY u.id"}
{"action": "detach", "schema_name": "a"}
```

## 設計メモ

- **同期 API** — `better-sqlite3` は同期クライアントなので await なしで高速（MCP 経由でも 1 回の tool call で 1 往復）
- **`readonly: true` で安全化** — LLM に勝手に DDL/DML させたくない時に
- **prepared statement** — `query` / `execute` / `transaction` は常に `.prepare()` + bind param。**ただし** `sql` 本文は LLM が書く文字列なので、bind されない箇所での injection 自体は防げない。`SQLITE_ALLOWED_ROOT` / PRAGMA allowlist / 識別子バリデーションで多層に絞っている前提
- **トランザクションは atomic 限定** — `transaction` アクションは `statements[]` を 1 ターン内で atomic 実行する。MCP ターンをまたいで `BEGIN ... COMMIT` を分割する手動制御は出来ない（接続が同じ better-sqlite3 ハンドルなのでターンをまたぐと状態が壊れやすい為）
- **`execute_script` の戻り値** — `{ ok: true }` のみ。ROW を返したい場合は分けて `query` を呼ぶ
- **import_csv は atomic** — DROP / CREATE / INSERT 全部を 1 つの transaction でラップ。途中の型エラー等で全部巻き戻る

## Attribution

- [better-sqlite3](https://github.com/WiseLibs/better-sqlite3) — 高速な同期 SQLite バインディング
- [SQLite](https://www.sqlite.org/)
- [Model Context Protocol](https://modelcontextprotocol.io/)

## ライセンス

MIT License © 2026 cUDGk — 詳細は [LICENSE](LICENSE) を参照。

## Changelog

## v0.3.0 — security & robustness

- **FS confinement** — `open` / `attach` / `import_csv` / `export_csv` / `export_json` / `vacuum INTO` / `backup` の path は `SQLITE_ALLOWED_ROOT` 配下に強制。`path.resolve` + `path.relative` 判定なので `..` でも抜け出せない。未設定時の既定はサーバ CWD
- **識別子バリデーション** — `schema(table=...)` / `import_csv` / `attach` / `detach` 等で組み立てる識別子は全て `quoteIdent()`（`/^[A-Za-z_][A-Za-z0-9_$]*$/` + `"` ダブル化）を通す。`PRAGMA table_info` 系は `pragma_table_info(?)` テーブル関数 + bind param に切替
- **PRAGMA allowlist** — 読み取り中心の安全な PRAGMA のみ既定許可。`writable_schema` / `temp_store_directory` / `data_store_directory` は常時ブロック。それ以外を実行したい場合は `SQLITE_ALLOW_DANGEROUS=1`
- **`execute_script` のガード** — 既定では `ATTACH DATABASE` / `PRAGMA writable_schema` を含むスクリプトを拒否（コメント除去後に走査）
- **行数ハードキャップ** — `query` の `limit` は既定 `SQLITE_MAX_LIMIT=100000` で上限。さらに `stmt.iterate()` を使い `limit + 1` で打ち切るので大きい結果セットでも OOM しない
- **その他** — `busy_timeout=5000` / `max_recursion_depth=1000` を毎オープン時に設定。SQLite エラーには `err.code` を含めて返す。SIGINT / SIGTERM で全 DB を確実にクローズ。`server.version` を `package.json` から取得
- **新アクション** — `dump_sql`：`sqlite_master` の `sql` を全部集めてスキーマダンプ用に返す

## v0.2.1 修正

Claude Code の LLM ツール呼び出しパスで、object / array 型の引数が JSON 文字列化された状態で届く事があるバグに対応。文字列で受け取っても `coerceObject()` ヘルパで解釈し直すようにし、zod schema は `z.union([<本来>, z.string()])` に緩和した。正常な object / array 経路は従来通り動作する。
