#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "Error: kubectl no está instalado o no está en PATH."
  exit 1
fi

FILES=(
  "dds-router-pub.yaml"
  "dds-discovery-server-pub.yaml"
  "dds-apps-publisher-1.yaml"
  "dds-apps-publisher-2.yaml"
  "dds-apps-publisher-3.yaml"
  "dds-monitor-pub.yaml"
)
APP_DEPLOYMENT="fastdds-publisher-mag30-deploy"
APP_CONTAINER="publisher-mag30"

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
