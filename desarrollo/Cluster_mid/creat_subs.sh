#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "Error: kubectl no está instalado o no está en PATH."
  exit 1
fi

FILES=(
  "dds-discovery-server-mid.yaml"
  "dds-router-mid.yaml"
  "dds-apps-aggregator-1.yaml"
  "dds-apps-aggregator-2.yaml"
  "dds-monitor-mid.yaml"
)
APP_DEPLOYMENT="fastdds-intermediate-store-deploy"
APP_CONTAINER="intermediate-store"

echo "Eliminando deployments existentes ..."
kubectl delete deployment --all

for file in "${FILES[@]}"; do
  full_path="$SCRIPT_DIR/$file"

  if [[ ! -f "$full_path" ]]; then
    echo "Error: no se encontró $full_path"
    exit 1
  fi

  echo "Aplicando $file ..."
  kubectl apply -f "$full_path"
done

echo "Despliegue completado en orden."
echo "Esperando a que la app este lista..."
kubectl rollout status "deployment/$APP_DEPLOYMENT" --timeout=120s

echo "Mostrando logs de la app (Ctrl+C para salir)..."
kubectl logs -f "deployment/$APP_DEPLOYMENT" -c "$APP_CONTAINER"
