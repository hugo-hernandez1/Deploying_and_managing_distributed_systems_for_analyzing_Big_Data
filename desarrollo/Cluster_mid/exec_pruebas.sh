#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SNIFFER_POD="net-sniffer-mid"
SNIFFER_YAML="$SCRIPT_DIR/sniffer.yaml"
CAPTURE_SECONDS=2

if ! command -v kubectl >/dev/null 2>&1; then
  echo "Error: kubectl no esta instalado o no esta en PATH."
  exit 1
fi

echo "Listado de pods:"
kubectl get pods -o wide

if kubectl get pod "$SNIFFER_POD" >/dev/null 2>&1; then
  echo "El pod $SNIFFER_POD ya existe."
else
  if [[ ! -f "$SNIFFER_YAML" ]]; then
    echo "Error: no se encontro $SNIFFER_YAML"
    exit 1
  fi

  echo "El pod $SNIFFER_POD no existe. Creandolo..."
  kubectl apply -f "$SNIFFER_YAML"
fi

echo "Esperando a que $SNIFFER_POD este Ready..."
kubectl wait --for=condition=Ready "pod/$SNIFFER_POD" --timeout=60s

echo "Ejecutando tcpdump durante $CAPTURE_SECONDS segundos..."
kubectl exec -i "$SNIFFER_POD" -- sh -c "
if command -v timeout >/dev/null 2>&1; then
  timeout -s INT ${CAPTURE_SECONDS}s tcpdump -ni any 'udp' -vv || true
else
  tcpdump -ni any 'udp' -vv &
  PID=\$!
  sleep $CAPTURE_SECONDS
  kill -INT \$PID 2>/dev/null || true
  wait \$PID 2>/dev/null || true
fi
"

echo "Captura finalizada."
