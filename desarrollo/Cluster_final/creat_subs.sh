#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "Error: kubectl no está instalado o no está en PATH."
  exit 1
fi

MONITORING_FILE="monitoring-stack-final.yaml"
DDS_FILES=(
  "dds-discovery-server-final.yaml"
  "dds-router-final.yaml"
  "dds-apps-final-1.yaml"
  "dds-monitor-final.yaml"
)
APP_DEPLOYMENT="fastdds-final-agg-sub-deploy"
APP_CONTAINER="final-agg-subscriber"
INFLUX_ORG="tfm"
INFLUX_BUCKET="air_quality"
INFLUX_TOKEN="tfm-influx-admin-token-changeme"
INFLUX_HOST="http://localhost:8086"

# ── 1. Monitoring stack (InfluxDB + Grafana) ──────────────────────────────
echo "==> [1/4] Aplicando monitoring stack ..."
kubectl apply -f "$SCRIPT_DIR/$MONITORING_FILE"

echo "Esperando a que InfluxDB esté listo ..."
kubectl rollout status deployment/influxdb --timeout=180s
kubectl wait --for=condition=ready pod -l app=influxdb --timeout=180s
echo "Esperando a que la API de InfluxDB esté disponible ..."
until kubectl exec deployment/influxdb -- influx ping --host http://localhost:8086 >/dev/null 2>&1; do
  echo "  InfluxDB aún inicializando, esperando 3s ..."
  sleep 3
done
echo "Esperando propagación DNS (10s) ..."
sleep 10
echo "InfluxDB listo."
echo "==> [2/4] Borrando datos previos de InfluxDB ..."
kubectl exec deployment/influxdb -- influx delete \
  --host "$INFLUX_HOST" \
  --org "$INFLUX_ORG" \
  --token "$INFLUX_TOKEN" \
  --bucket "$INFLUX_BUCKET" \
  --start 1970-01-01T00:00:00Z \
  --stop 2100-01-01T00:00:00Z

echo "Datos previos borrados del bucket '$INFLUX_BUCKET'."

# ── 2. DDS stack ─────────────────────────────────────────────────────────
echo "==> [3/4] Eliminando deployments DDS existentes ..."
kubectl delete deployment discovery-server dds-router-final fastdds-final-agg-sub-deploy --ignore-not-found

echo "Aplicando DDS stack ..."
for file in "${DDS_FILES[@]}"; do
  full_path="$SCRIPT_DIR/$file"
  if [[ ! -f "$full_path" ]]; then
    echo "Error: no se encontró $full_path"
    exit 1
  fi
  echo "Aplicando $file ..."
  kubectl apply -f "$full_path"
done

# ── 3. Esperar y mostrar logs ─────────────────────────────────────────────
echo "==> [4/4] Esperando a que la app esté lista ..."
kubectl rollout status "deployment/$APP_DEPLOYMENT" --timeout=120s

echo "Mostrando logs de la app (Ctrl+C para salir)..."
kubectl logs -f "deployment/$APP_DEPLOYMENT" -c "$APP_CONTAINER"