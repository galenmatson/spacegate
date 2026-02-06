# Spacegate Web (v0.1)

Minimal React + Vite frontend for browsing Spacegate systems.

## Run

```bash
cd /data/spacegate/services/web
npm install

# Point at API if running on another host/port
export VITE_API_BASE=http://localhost:8000

# Or use dev proxy (default targets http://127.0.0.1:8000)
export VITE_API_PROXY=http://127.0.0.1:8000

npm run dev
```

Open `http://localhost:5173` in a browser.
