import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const apiProxy = env.VITE_API_PROXY || "http://127.0.0.1:8000";
  const mapTilesProxy = env.VITE_MAP_TILES_PROXY || "";
  const proxy = {
    "/api": {
      target: apiProxy,
      changeOrigin: true
    }
  };
  if (mapTilesProxy) {
    proxy["/map-tiles"] = {
      target: mapTilesProxy,
      changeOrigin: true,
      secure: false
    };
  }

  return {
    plugins: [react()],
    server: {
      port: 5173,
      host: true,
      proxy
    }
  };
});
