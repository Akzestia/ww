# ww — Scylla‑backed secrets daemon + CLI

`ww` is a **Doppler‑like secret manager** built on [ScyllaDB](https://www.scylladb.com/). It runs a background daemon that talks to Scylla and exposes project secrets over a local **Unix socket**. The companion **CLI** can inject environment variables into commands, download them in multiple formats, or update individual keys.

---

## ✨ Features

- 🔑 Store project secrets in ScyllaDB (`map<text,text>` column)
- 📡 Local daemon with Unix socket protocol
- 🛠 CLI with familiar commands: `run`, `secrets`, `set-env`, `client`
- 🌐 Path‑to‑project bindings (`~/.config/ww/config.toml`)
- 📄 Export secrets as `.env`, JSON, or YAML
- 🔒 Socket permissions default to `0600`

---

## 🚀 Quick Start

### 1. Start the daemon

```bash
ww serve --node 127.0.0.1:9042 --user cassandra --pass cassandra
```

### 2. Bind your repo to a project

```bash
cd /path/to/repo
ww client setup my_project
```

### 3. Set a secret

```bash
ww set-env --name API_KEY --val abc123
```

### 4. Run your app with secrets injected

```bash
ww node app.js
```

---

## 🔧 CLI Overview

```text
USAGE:
    ww <COMMAND>

COMMANDS:
    serve       Start the daemon server
    run         Run a command with env vars injected
    secrets     Download or read secrets
    set-env     Upsert a single secret
    client      Manage global client config
```

### Examples

- **Show current project resolution**:

  ```bash
  ww client which
  ```

- **Download secrets to `.env` file**:

  ```bash
  ww secrets download --format env --output .env
  ```

- **Fetch a single secret**:

  ```bash
  ww secrets get --name API_KEY --plain
  ```

- **Run any command, injecting resolved project secrets**:

  ```bash
  ww npm run dev
  ```

---

## ⚙️ Client Config

Location: `~/.config/ww/config.toml`

```toml
default_project = "demo_project"

[[paths]]
dir = "/abs/path/to/repo"
project = "my_project"
```

Resolution order:

1. `--project` flag (explicit)
2. `WW_PROJECT` environment variable
3. Path binding from config
4. `default_project`

---

## 📡 Socket Protocol

The daemon speaks a simple line protocol over a Unix socket:

- `QUERY_PROJECTS:<project>\n` → `KEY=VALUE\n...`
- `ENSURE_PROJECT:<project>\n` → `OK\n` or `ERROR:...`
- `SET_ENV:<project>:<key>=<value>\n` → `OK\n` or `ERROR:...`

Socket path defaults to `/tmp/ww.sock`.

---

## 🔐 Security Notes

- Socket is `0600` (owner read/write only)
- Assumes local single‑user trust; no encryption/auth on socket
- Use `--plain` only when piping into secure sinks

---

## 🛠 Development

### Build

```bash
cargo build --release
```

### Test

```bash
cargo test
```

### Install locally

```bash
cargo install --path .
```

---

## 📜 License

Licensed under [MIT](LICENSE).
