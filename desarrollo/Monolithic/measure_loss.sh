#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Uso: $0 <duracion_minutos>"
  echo "Ejemplo: $0 5"
  exit 1
fi

DURATION_MINUTES="$1"

if ! [[ "$DURATION_MINUTES" =~ ^[0-9]+$ ]] || [[ "$DURATION_MINUTES" -le 0 ]]; then
  echo "Error: la duración debe ser un entero positivo en minutos."
  exit 1
fi

DURATION_SECONDS=$(( DURATION_MINUTES * 60 ))

INFLUX_DEPLOYMENT="${INFLUX_DEPLOYMENT:-influxdb}"
INFLUX_HOST="${INFLUX_HOST:-http://localhost:8086}"
INFLUX_ORG="${INFLUX_ORG:-tfm}"
INFLUX_BUCKET="${INFLUX_BUCKET:-air_quality}"
INFLUX_TOKEN="${INFLUX_TOKEN:-tfm-influx-admin-token-changeme}"

RAW_MEASUREMENT="${RAW_MEASUREMENT:-raw_measurements}"
AGG_MEASUREMENT="${AGG_MEASUREMENT:-agg_stats}"

# Ajusta estos campos a los reales de Influx
RAW_COUNT_FIELD="${RAW_COUNT_FIELD:-value}"
AGG_COUNT_FIELD="${AGG_COUNT_FIELD:-count}"

PUBLISHER_DEPLOYMENTS=(
  "fastdds-publisher-mag12-deploy"
  "fastdds-publisher-mag20-deploy"
  "fastdds-publisher-mag30-deploy"
)

ANALYSIS_DEPLOYMENT="${ANALYSIS_DEPLOYMENT:-fastdds-data-analysis-deploy}"

# Ajusta estas regex a tus logs reales
RAW_SENT_REGEX="${RAW_SENT_REGEX:-^\[TX\]|Published sample|total_sent=|air_quality_raw}"
AGG_SENT_REGEX="${AGG_SENT_REGEX:-^\[TX_AGG\]|Published aggregate|total_published=|air_quality_agg}"

OUT_DIR="${OUT_DIR:-results}"
mkdir -p "$OUT_DIR"

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Error: falta el comando '$1'"
    exit 1
  }
}

query_influx_count() {
  local measurement="$1"
  local field="$2"
  local start="$3"
  local stop="$4"

  kubectl exec "deployment/${INFLUX_DEPLOYMENT}" -- \
    influx query \
      --host "$INFLUX_HOST" \
      --org "$INFLUX_ORG" \
      --token "$INFLUX_TOKEN" \
"from(bucket: \"${INFLUX_BUCKET}\")
  |> range(start: ${start}, stop: ${stop})
  |> filter(fn: (r) => r._measurement == \"${measurement}\")
  |> filter(fn: (r) => r._field == \"${field}\")
  |> count()
" 2>/dev/null | awk '
    /^[[:space:]]*[0-9]+[[:space:]]*$/ { v=$1 }
    END {
      if (v == "") v=0
      print v
    }'
}

count_raw_sent_from_logs() {
  local since_time="$1"
  local total=0
  local dep count

  for dep in "${PUBLISHER_DEPLOYMENTS[@]}"; do
    count="$(
      kubectl logs "deployment/${dep}" --since-time="$since_time" 2>/dev/null \
        | grep -E -c "${RAW_SENT_REGEX}" || true
    )"
    total=$(( total + count ))
  done

  echo "$total"
}

count_agg_sent_from_logs() {
  local since_time="$1"
  kubectl logs "deployment/${ANALYSIS_DEPLOYMENT}" --since-time="$since_time" 2>/dev/null \
    | grep -E -c "${AGG_SENT_REGEX}" || true
}

need_cmd kubectl
need_cmd awk
need_cmd grep
need_cmd date
need_cmd sleep

START_ISO="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

echo "Inicio de ventana de medida: ${START_ISO}"
echo "Duración: ${DURATION_MINUTES} min (${DURATION_SECONDS} s)"
echo "Midiendo solo lo ocurrido dentro de esta ventana..."
sleep "$DURATION_SECONDS"

STOP_ISO="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

RAW_SENT="$(count_raw_sent_from_logs "$START_ISO")"
RAW_RECV="$(query_influx_count "$RAW_MEASUREMENT" "$RAW_COUNT_FIELD" "$START_ISO" "$STOP_ISO")"

AGG_SENT="$(count_agg_sent_from_logs "$START_ISO")"
AGG_RECV="$(query_influx_count "$AGG_MEASUREMENT" "$AGG_COUNT_FIELD" "$START_ISO" "$STOP_ISO")"

RAW_LOST=$(( RAW_SENT - RAW_RECV ))
AGG_LOST=$(( AGG_SENT - AGG_RECV ))
TOTAL_SENT=$(( RAW_SENT + AGG_SENT ))
TOTAL_RECV=$(( RAW_RECV + AGG_RECV ))
TOTAL_LOST=$(( TOTAL_SENT - TOTAL_RECV ))

(( RAW_LOST < 0 )) && RAW_LOST=0
(( AGG_LOST < 0 )) && AGG_LOST=0
(( TOTAL_LOST < 0 )) && TOTAL_LOST=0

RAW_LOSS_RATE="0"
AGG_LOSS_RATE="0"
TOTAL_LOSS_RATE="0"

if (( RAW_SENT > 0 )); then
  RAW_LOSS_RATE="$(awk -v l="$RAW_LOST" -v s="$RAW_SENT" 'BEGIN { printf "%.6f", (l/s)*100 }')"
fi
if (( AGG_SENT > 0 )); then
  AGG_LOSS_RATE="$(awk -v l="$AGG_LOST" -v s="$AGG_SENT" 'BEGIN { printf "%.6f", (l/s)*100 }')"
fi
if (( TOTAL_SENT > 0 )); then
  TOTAL_LOSS_RATE="$(awk -v l="$TOTAL_LOST" -v s="$TOTAL_SENT" 'BEGIN { printf "%.6f", (l/s)*100 }')"
fi

TIMESTAMP="$(date -u +"%Y%m%dT%H%M%SZ")"
REPORT_TXT="${OUT_DIR}/loss_report_${TIMESTAMP}.txt"
REPORT_CSV="${OUT_DIR}/loss_report_${TIMESTAMP}.csv"

cat > "$REPORT_TXT" <<EOF
Ventana:
  start=${START_ISO}
  stop=${STOP_ISO}
  duration_minutes=${DURATION_MINUTES}

RAW:
  sent=${RAW_SENT}
  recv=${RAW_RECV}
  lost=${RAW_LOST}
  loss_rate_percent=${RAW_LOSS_RATE}

AGG:
  sent=${AGG_SENT}
  recv=${AGG_RECV}
  lost=${AGG_LOST}
  loss_rate_percent=${AGG_LOSS_RATE}

TOTAL:
  sent=${TOTAL_SENT}
  recv=${TOTAL_RECV}
  lost=${TOTAL_LOST}
  loss_rate_percent=${TOTAL_LOSS_RATE}

Config:
  RAW_MEASUREMENT=${RAW_MEASUREMENT}
  RAW_COUNT_FIELD=${RAW_COUNT_FIELD}
  AGG_MEASUREMENT=${AGG_MEASUREMENT}
  AGG_COUNT_FIELD=${AGG_COUNT_FIELD}
  RAW_SENT_REGEX=${RAW_SENT_REGEX}
  AGG_SENT_REGEX=${AGG_SENT_REGEX}
EOF

cat > "$REPORT_CSV" <<EOF
scope,sent,recv,lost,loss_rate_percent,start,stop,duration_minutes
raw,${RAW_SENT},${RAW_RECV},${RAW_LOST},${RAW_LOSS_RATE},${START_ISO},${STOP_ISO},${DURATION_MINUTES}
agg,${AGG_SENT},${AGG_RECV},${AGG_LOST},${AGG_LOSS_RATE},${START_ISO},${STOP_ISO},${DURATION_MINUTES}
total,${TOTAL_SENT},${TOTAL_RECV},${TOTAL_LOST},${TOTAL_LOSS_RATE},${START_ISO},${STOP_ISO},${DURATION_MINUTES}
EOF

cat "$REPORT_TXT"
echo
echo "Informe guardado en:"
echo "  ${REPORT_TXT}"
echo "  ${REPORT_CSV}"