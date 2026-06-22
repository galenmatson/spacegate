#!/usr/bin/env sh
set -eu

TLS_CERT=/etc/spacegate/tls/tls.crt
TLS_KEY=/etc/spacegate/tls/tls.key

if [ -r "$TLS_CERT" ] && [ -r "$TLS_KEY" ]; then
  cp /etc/nginx/spacegate/nginx-tls.conf /etc/nginx/conf.d/default.conf
  echo "Spacegate nginx: TLS enabled"
else
  cp /etc/nginx/spacegate/nginx-http.conf /etc/nginx/conf.d/default.conf
  echo "Spacegate nginx: TLS disabled; cert/key not mounted"
fi
