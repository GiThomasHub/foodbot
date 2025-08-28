#!/usr/bin/env bash
set -euo pipefail

# === Konfiguration ===
PROJECT="foodapp-463218"
REGION="europe-west6"
SERVICE="foodbot"

echo "Project=$PROJECT  Region=$REGION  Service=$SERVICE"

# === Helper: Hole Service-URL (soll PUBLIC_URL/BASE_URL sein) ===
SERVICE_URL="$(gcloud run services describe "$SERVICE" --region "$REGION" --format='value(status.url)')"
if [[ -z "${SERVICE_URL:-}" ]]; then
  echo "❌ Konnte Service-URL nicht ermitteln."
  exit 2
fi
echo "Service URL: $SERVICE_URL"

# === Kandidaten- & stabile Revision ermitteln ===
CAND="$(gcloud run services describe "$SERVICE" --region "$REGION" --format='value(status.latestCreatedRevisionName)')"
STABLE="$(gcloud run services describe "$SERVICE" --region "$REGION" --format='value(status.latestReadyRevisionName)')"
echo "Candidate=$CAND  Stable=$STABLE"

if [[ -z "${CAND:-}" ]]; then
  echo "❌ Keine Kandidaten-Revision gefunden."
  exit 2
fi

# === Auf Ready warten (max. ~8min) ===
echo "⏳ Warte auf Ready für $CAND ..."
for i in {1..80}; do
  READY="$(gcloud run revisions describe "$CAND" --region "$REGION" --format="value(status.conditions[?type='Ready'].status)")"
  REASON="$(gcloud run revisions describe "$CAND" --region "$REGION" --format="value(status.conditions[?type='Ready'].reason)")"
  MSG="$(gcloud run revisions describe "$CAND" --region "$REGION" --format="value(status.conditions[?type='Ready'].message)")"
  echo "Iter $i: Ready=$READY Reason=${REASON:-}"; 

  if [[ "$READY" == "True" ]]; then
    echo "✅ $CAND ist Ready."
    break
  fi

  if [[ "${REASON:-}" == "HealthCheckContainerError" ]]; then
    echo "❌ Health-Check-Fehler: $MSG"
    echo "↩️  Keinen Traffic verschieben. Bleibe auf Stable=$STABLE."
    exit 3
  fi
  sleep 6
done

# Safety: finaler Ready-Check
FINAL_READY="$(gcloud run revisions describe "$CAND" --region "$REGION" --format="value(status.conditions[?type='Ready'].status)")"
if [[ "$FINAL_READY" != "True" ]]; then
  echo "❌ $CAND wurde nicht Ready. Abbruch ohne Traffic-Shift."
  exit 4
fi

# === Prüfen/Setzen PUBLIC_URL & BASE_URL ===
# Wir prüfen die aktuell aktiven Env-Vars (vom Service-Template).
CUR_PUBLIC="$(gcloud run services describe "$SERVICE" --region "$REGION" --format="value(spec.template.spec.containers[0].env[?name=='PUBLIC_URL'].value)")"
CUR_BASE="$(gcloud run services describe "$SERVICE" --region "$REGION" --format="value(spec.template.spec.containers[0].env[?name=='BASE_URL'].value)")"

NEED_UPDATE="no"
if [[ -z "${CUR_PUBLIC:-}" || "$CUR_PUBLIC" != "$SERVICE_URL" ]]; then
  echo "ℹ️ PUBLIC_URL ist leer/anders (aktuell='$CUR_PUBLIC') → setze auf $SERVICE_URL"
  NEED_UPDATE="yes"
fi
if [[ -z "${CUR_BASE:-}" || "$CUR_BASE" != "$SERVICE_URL" ]]; then
  echo "ℹ️ BASE_URL ist leer/anders (aktuell='$CUR_BASE') → setze auf $SERVICE_URL"
  NEED_UPDATE="yes"
fi

if [[ "$NEED_UPDATE" == "yes" ]]; then
  echo "🔧 Update Env-Vars (ohne Traffic-Shift) ..."
  gcloud run services update "$SERVICE" \
    --region "$REGION" \
    --no-traffic \
    --set-env-vars="PUBLIC_URL=${SERVICE_URL},BASE_URL=${SERVICE_URL},ROLL_OUT=$(date +%s)" >/dev/null

  # neue Kandidaten-Revision nach Env-Update
  CAND="$(gcloud run services describe "$SERVICE" --region "$REGION" --format='value(status.latestCreatedRevisionName)')"
  echo "Neue Candidate-Revision nach Env-Update: $CAND"

  # Wieder auf Ready warten
  for i in {1..80}; do
    READY="$(gcloud run revisions describe "$CAND" --region "$REGION" --format="value(status.conditions[?type='Ready'].status)")"
    REASON="$(gcloud run revisions describe "$CAND" --region "$REGION" --format="value(status.conditions[?type='Ready'].reason)")"
    echo "Iter(EnvFix) $i: Ready=$READY Reason=${REASON:-}"
    [[ "$READY" == "True" ]] && break
    [[ "${REASON:-}" == "HealthCheckContainerError" ]] && { echo "❌ Env-Fix Revision hat Health-Error."; exit 5; }
    sleep 6
  done
fi

# === Traffic verschieben ===
echo "🚦 Setze Traffic 100% → $CAND"
gcloud run services update-traffic "$SERVICE" --region "$REGION" --to-revisions "$CAND=100" >/dev/null

# === Health-Checks ===
URL="$(gcloud run services describe "$SERVICE" --region "$REGION" --format='value(status.url)')"
echo "Ping health: $URL/webhook/health"
if curl -fsS "$URL/webhook/health" >/dev/null; then
  echo "✅ /webhook/health OK"
else
  echo "❌ /webhook/health fehlgeschlagen → Rollback auf $STABLE"
  gcloud run services update-traffic "$SERVICE" --region "$REGION" --to-revisions "$STABLE=100" >/dev/null
  exit 6
fi

# === Optional: Requests-Log der letzten Minuten prüfen ===
echo "🔎 Letzte Webhook-Requests:"
gcloud logging read \
  "resource.type=cloud_run_revision AND resource.labels.service_name=$SERVICE AND logName=projects/$PROJECT/logs/run.googleapis.com%2Frequests" \
  --limit=5 --order=desc \
  --format='table(timestamp, httpRequest.requestMethod, httpRequest.requestUrl, httpRequest.status, resource.labels.revision_name)' || true

echo "🎉 Promotion fertig."
