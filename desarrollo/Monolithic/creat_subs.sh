#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "Error: kubectl no está instalado o no está en PATH."
  exit 1
fi

MONITORING_FILE="monitoring-stack-final.yaml"

FIRST_DDS_FILES=(
  "dds-discovery-server-final.yaml"
  "dds-monitor-mono.yaml"
)

REMAINING_DDS_FILES=(
  "dds-apps-final-1.yaml"
  "dds-apps-aggregator-1.yaml"
  "dds-apps-aggregator-2.yaml"
  "dds-apps-publisher-1.yaml"
  "dds-apps-publisher-2.yaml"
  "dds-apps-publisher-3.yaml"
)

DEPLOYMENTS=(
  "discovery-server"
  # "fastdds-monitor"
  "influxdb"
  "grafana"
  "fastdds-final-agg-sub-deploy"
  "fastdds-intermediate-store-deploy"
  "fastdds-data-analysis-deploy"
  "fastdds-publisher-mag12-deploy"
  "fastdds-publisher-mag20-deploy"
  "fastdds-publisher-mag30-deploy"
)

INFLUX_ORG="tfm"
INFLUX_BUCKET="air_quality"
INFLUX_TOKEN="tfm-influx-admin-token-changeme"
INFLUX_HOST="http://localhost:8086"

echo "==> [1/6] Aplicando monitoring stack ..."
kubectl apply -f "$SCRIPT_DIR/$MONITORING_FILE"

echo "Esperando a que InfluxDB esté listo ..."
kubectl rollout status deployment/influxdb --timeout=180s
kubectl wait --for=condition=ready pod -l app=influxdb --timeout=180s

echo "Esperando a que la API de InfluxDB esté disponible ..."
until kubectl exec deployment/influxdb -- influx ping --host "$INFLUX_HOST" >/dev/null 2>&1; do
  echo "  InfluxDB aún inicializando, esperando 3s ..."
  sleep 3
done

echo "Esperando propagación DNS (10s) ..."
sleep 10
echo "InfluxDB listo."

echo "==> [2/6] Borrando datos previos de InfluxDB ..."
kubectl exec deployment/influxdb -- influx delete \
  --host "$INFLUX_HOST" \
  --org "$INFLUX_ORG" \
  --token "$INFLUX_TOKEN" \
  --bucket "$INFLUX_BUCKET" \
  --start 1970-01-01T00:00:00Z \
  --stop 2100-01-01T00:00:00Z

echo "Datos previos borrados del bucket '$INFLUX_BUCKET'."

echo "==> [3/6] Limpiando despliegues anteriores ..."
kubectl delete deployment \
  discovery-server \
  fastdds-monitor \
  fastdds-publisher-mag12-deploy \
  fastdds-publisher-mag20-deploy \
  fastdds-publisher-mag30-deploy \
  fastdds-intermediate-store-deploy \
  fastdds-data-analysis-deploy \
  fastdds-final-agg-sub-deploy \
  --ignore-not-found

echo "==> [4/6] Aplicando Discovery Server y Monitor DDS ..."
for file in "${FIRST_DDS_FILES[@]}"; do
  full_path="$SCRIPT_DIR/$file"
  if [[ ! -f "$full_path" ]]; then
    echo "Error: no se encontró $full_path"
    exit 1
  fi
  echo "Aplicando $file ..."
  kubectl apply -f "$full_path"
done

echo "Esperando a que Discovery Server esté listo ..."
kubectl rollout status deployment/discovery-server --timeout=180s
kubectl wait --for=condition=ready pod -l app=discovery-server --timeout=180s

# echo "Esperando a que el Monitor DDS esté listo ..."
# kubectl rollout status deployment/fastdds-monitor --timeout=180s
# kubectl wait --for=condition=ready pod -l app=fastdds-monitor --timeout=180s

# echo
# echo "Monitor DDS desplegado. Logs iniciales:"
# echo "------------------------------------------------------------"
# kubectl logs deployment/fastdds-monitor --tail=20 || true
# echo "------------------------------------------------------------"
# echo
# read -r -p "Pulsa Enter para continuar con el resto del stack DDS ..."

echo "==> [5/6] Aplicando el resto del stack DDS ..."
for file in "${REMAINING_DDS_FILES[@]}"; do
  full_path="$SCRIPT_DIR/$file"
  if [[ ! -f "$full_path" ]]; then
    echo "Error: no se encontró $full_path"
    exit 1
  fi
  echo "Aplicando $file ..."
  kubectl apply -f "$full_path"
done

echo "==> [6/6] Esperando a que todos los deployments estén listos ..."
for dep in "${DEPLOYMENTS[@]}"; do
  if kubectl get deployment "$dep" >/dev/null 2>&1; then
    echo "  Esperando $dep ..."
    kubectl rollout status "deployment/$dep" --timeout=180s
  fi
done

echo
echo "Todos los deployments están listos."
echo
echo "Pods desplegados:"
kubectl get pods -o wide

echo
echo "Para seguir los logs del monitor en tiempo real:"
echo "  kubectl logs -f deployment/fastdds-monitor"