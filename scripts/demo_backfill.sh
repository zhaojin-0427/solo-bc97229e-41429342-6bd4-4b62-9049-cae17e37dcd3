#!/usr/bin/env bash
# End-to-end demo of the background backfill feature against two running
# services (defaults match docker-compose):
#   - ingest API + worker:  http://localhost:8000
#   - local source service: http://localhost:8100
#
# It walks through:
#   1. source manifest pagination (1-20 items, empty cursor = end)
#   2. a full background backfill across multiple pages
#   3. injected mid-stream disconnect -> job pauses, no half page
#   4. resume from the persisted cursor -> study completes
#   5. injected corrupted bytes -> byte-conflict pause -> reuse on resume
#   6. progress / error / attempt-history and rewritten DICOM download
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
SOURCE_URL="${SOURCE_URL:-http://localhost:8100}"
STUDY_UID="${STUDY_UID:-1.9.84.20261112.80.1}"
CENTER="demo-bf"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

say() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
PY="$(command -v python3)"
json() { "$PY" -c "import json,sys;print(json.load(sys.stdin)$1)"; }
wait_status() { # job_id expected...
  local job="$1"; shift
  local wanted="$*"
  for _ in $(seq 1 200); do
    local s
    s=$(curl -sS "$BASE_URL/api/v1/backfills/$job" | json "['status']")
    for w in $wanted; do [ "$s" = "$w" ] && { echo "$s"; return 0; }; done
    sleep 0.25
  done
  echo "TIMEOUT waiting for $wanted (last=$s)" >&2
  return 1
}

say "0. health checks"
curl -sS "$BASE_URL/healthz"; echo
curl -sS "$SOURCE_URL/healthz"; echo

say "1. SOURCE MANIFEST PAGINATION (page_size 20, empty next_cursor ends it)"
curl -sS "$SOURCE_URL/manifest?study_uid=$STUDY_UID&cursor=&page_size=20" \
  | python3 -m json.tool | head -20

say "2. SUBMIT A BACKGROUND BACKFILL (page_size 10)"
JOB=$(curl -sS -X POST "$BASE_URL/api/v1/backfills" -H 'Content-Type: application/json' \
  -d "{\"center_id\":\"$CENTER\",\"original_study_uid\":\"$STUDY_UID\",\"source_base_url\":\"$SOURCE_URL\",\"page_size\":10}" \
  | json "['job_id']")
echo "job_id=$JOB"
wait_status "$JOB" completed > /dev/null
curl -sS "$BASE_URL/api/v1/backfills/$JOB" | python3 -m json.tool

say "3. INJECT A MID-STREAM DISCONNECT on the first SOP, new center"
FIRST_SOP=$(curl -sS "$SOURCE_URL/manifest?study_uid=$STUDY_UID&page_size=1" \
  | json "['items'][0]['sop_uid']")
curl -sS -X POST "$SOURCE_URL/faults" -H 'Content-Type: application/json' \
  -d "{\"sop_uid\":\"$FIRST_SOP\",\"kind\":\"disconnect\"}" > /dev/null
JOB2=$(curl -sS -X POST "$BASE_URL/api/v1/backfills" -H 'Content-Type: application/json' \
  -d "{\"center_id\":\"demo-disc\",\"original_study_uid\":\"$STUDY_UID\",\"source_base_url\":\"$SOURCE_URL\",\"page_size\":10}" \
  | json "['job_id']")
wait_status "$JOB2" paused > /dev/null
echo "-- paused job (note pages/instances stay 0: no half page, cursor not advanced)"
curl -sS "$BASE_URL/api/v1/backfills/$JOB2" | python3 -m json.tool
echo "-- batches/instances for the center must be empty"
curl -sS "$BASE_URL/api/v1/batches?center_id=demo-disc"; echo
curl -sS "$BASE_URL/api/v1/instances?center_id=demo-disc"; echo

say "4. CLEAR THE FAULT AND RESUME from the persisted cursor"
curl -sS -X DELETE "$SOURCE_URL/faults/$FIRST_SOP" > /dev/null
curl -sS -X POST "$BASE_URL/api/v1/backfills/$JOB2/resume" > /dev/null
wait_status "$JOB2" completed > /dev/null
curl -sS "$BASE_URL/api/v1/backfills/$JOB2" | python3 -m json.tool
echo "-- attempt history (one paused row, then committed pages)"
curl -sS "$BASE_URL/api/v1/backfills/$JOB2/attempts" | python3 -m json.tool

say "5. BYTE CONFLICT: corrupt bytes for an SOP already stored for demo-bf"
curl -sS -X POST "$SOURCE_URL/faults" -H 'Content-Type: application/json' \
  -d "{\"sop_uid\":\"$FIRST_SOP\",\"kind\":\"corrupt\"}" > /dev/null
JOB3=$(curl -sS -X POST "$BASE_URL/api/v1/backfills" -H 'Content-Type: application/json' \
  -d "{\"center_id\":\"$CENTER-2\",\"original_study_uid\":\"$STUDY_UID\",\"source_base_url\":\"$SOURCE_URL\",\"page_size\":10}" \
  | json "['job_id']")
# Seed healthy bytes first so the corrupted retransmit conflicts.
curl -sS -X DELETE "$SOURCE_URL/faults/$FIRST_SOP" > /dev/null
HEALTHY_URL=$(curl -sS "$SOURCE_URL/manifest?study_uid=$STUDY_UID&page_size=1" | json "['items'][0]['file_url']")
curl -sS "$SOURCE_URL$HEALTHY_URL" -o "$WORK/healthy.dcm"
curl -sS -X POST "$BASE_URL/api/v1/batches" -F "center_id=$CENTER-2" \
  -F "files=@$WORK/healthy.dcm" > /dev/null
curl -sS -X POST "$SOURCE_URL/faults" -H 'Content-Type: application/json' \
  -d "{\"sop_uid\":\"$FIRST_SOP\",\"kind\":\"corrupt\"}" > /dev/null
wait_status "$JOB3" paused > /dev/null
curl -sS "$BASE_URL/api/v1/backfills/$JOB3" \
  | python3 -c "import json,sys;d=json.load(sys.stdin);print('paused:',d['last_error_code'],'pages:',d['pages_committed'])"
curl -sS -X DELETE "$SOURCE_URL/faults/$FIRST_SOP" > /dev/null
curl -sS -X POST "$BASE_URL/api/v1/backfills/$JOB3/resume" > /dev/null
wait_status "$JOB3" completed > /dev/null
curl -sS "$BASE_URL/api/v1/backfills/$JOB3" \
  | python3 -c "import json,sys;d=json.load(sys.stdin);print('completed: pages',d['pages_committed'],'instances',d['instances_committed'],'reused',d['instances_reused'])"

say "6. DOWNLOAD A REWRITTEN DICOM and verify invariants"
IID=$(curl -sS "$BASE_URL/api/v1/instances?center_id=$CENTER&limit=1" | json "[0]['id']")
curl -sS "$BASE_URL/api/v1/instances/$IID/download" -o "$WORK/out.dcm"
python3 - "$WORK/out.dcm" <<'PY'
import sys, pydicom
ds = pydicom.dcmread(sys.argv[1])
assert str(ds.file_meta.TransferSyntaxUID) == "1.2.840.10008.1.2.1"
assert not ds.is_implicit_VR and ds.is_little_endian
print("OK: Explicit VR Little Endian preserved; rewritten SOP", ds.SOPInstanceUID)
PY

echo
echo "Interactive docs:"
echo "  ingest API + worker: $BASE_URL/docs"
echo "  source service:      $SOURCE_URL/docs"
