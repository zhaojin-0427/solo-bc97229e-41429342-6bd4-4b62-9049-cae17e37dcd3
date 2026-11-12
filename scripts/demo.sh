#!/usr/bin/env bash
# End-to-end demo against a running ingest API (default: http://localhost:8000).
#
# It walks through every requirement using the synthetic samples under
# samples/generated/:
#   1. cross-center UID collision  (same original UID, two centers)
#   2. reference-first, then the referenced image arrives (same center)
#   3. reference must NOT resolve across centers
#   4. idempotent retransmit (same bytes) vs. byte conflict (whole-batch reject)
#   5. batch query + object download, verifying pixels / transfer syntax
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
SAMPLES="$(cd "$(dirname "$0")/../samples/generated" && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

say() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
post_batch() { # center file...
  local center="$1"; shift
  curl -sS -X POST "$BASE_URL/api/v1/batches" \
    -F "center_id=$center" \
    $(printf -- '-F files=@%s ' "$@")
}

say "0. health check"
curl -sS "$BASE_URL/healthz"; echo

say "1. CROSS-CENTER UID COLLISION: both centers reuse the same original SOP UID"
post_batch center-a "$SAMPLES/cross_center_uid_collision/center-a/image_001.dcm" | tee "$WORK/a.json"; echo
post_batch center-b "$SAMPLES/cross_center_uid_collision/center-b/image_001.dcm" | tee "$WORK/b.json"; echo
UID_A=$(python3 -c 'import json;print(json.load(open("'$WORK/a.json'"))["files"][0]["new_sop_uid"])')
UID_B=$(python3 -c 'import json;print(json.load(open("'$WORK/b.json'"))["files"][0]["new_sop_uid"])')
[ "$UID_A" != "$UID_B" ] && echo "OK: same original UID -> distinct remapped UIDs per center"

say "2. REFERENCE-FIRST: upload the SR that points at an image not yet present"
post_batch center-x "$SAMPLES/reference_first/struct_001.dcm" | tee "$WORK/struct.json"; echo
echo "-- pending references for center-x (expect one unresolved):"
curl -sS "$BASE_URL/api/v1/references/pending?center_id=center-x&resolved=false"; echo

say "3. The SAME original target SOP uploaded by center-y must NOT resolve center-x's ref"
post_batch center-y "$SAMPLES/reference_first/image_001.dcm" > /dev/null
curl -sS "$BASE_URL/api/v1/references/pending?center_id=center-x&resolved=false"; echo
echo "  (still one pending row above: references never cross centers)"

say "4. Now center-x uploads its own image -> pending reference resolves"
post_batch center-x "$SAMPLES/reference_first/image_001.dcm" > /dev/null
curl -sS "$BASE_URL/api/v1/references/pending?center_id=center-x&resolved=false"; echo
echo "  (empty list above: reference resolved by same-center image)"

say "5. IDEMPOTENT RETRANSMIT: identical bytes reuse the stored object"
post_batch center-x "$SAMPLES/reference_first/image_001.dcm" | python3 -m json.tool

say "6. BYTE CONFLICT: same original SOP, changed bytes -> whole batch rejected (409)"
cp "$SAMPLES/reference_first/image_001.dcm" "$WORK/mutated.dcm"
printf '\001' | dd of="$WORK/mutated.dcm" bs=1 seek=$(( $(stat -c%s "$WORK/mutated.dcm") - 1 )) count=1 conv=notrunc 2>/dev/null
post_batch center-x "$WORK/mutated.dcm" "$SAMPLES/reference_first/struct_001.dcm"; echo

say "7. BATCH QUERY + DOWNLOAD: verify rewritten object keeps TS and pixel bytes"
BATCH=$(python3 -c 'import json;print(json.load(open("'$WORK/a.json'"))["batch_id"])')
curl -sS "$BASE_URL/api/v1/batches/$BATCH" | python3 -m json.tool
INSTANCE=$(curl -sS "$BASE_URL/api/v1/instances?center_id=center-a" | python3 -c 'import json,sys;print(json.load(sys.stdin)[0]["id"])')
curl -sS -D "$WORK/headers" "$BASE_URL/api/v1/instances/$INSTANCE/download" -o "$WORK/down.dcm"
grep -iE "content-type|x-new-sop|x-transfer" "$WORK/headers"
python3 - "$SAMPLES/cross_center_uid_collision/center-a/image_001.dcm" "$WORK/down.dcm" <<'PY'
import sys, pydicom
src, out = (pydicom.dcmread(p) for p in sys.argv[1:])
assert src.PixelData == out.PixelData, "pixel bytes changed!"
assert str(out.file_meta.TransferSyntaxUID) == "1.2.840.10008.1.2.1"
assert out.is_implicit_VR is False and out.is_little_endian is True
print("OK: pixel bytes identical; transfer syntax still Explicit VR Little Endian")
print("rewritten SOP:", out.SOPInstanceUID)
PY

say "8. INVALID OBJECT: whole batch rejected (422), nothing stored"
curl -sS -o "$WORK/inv.json" -w "HTTP %{http_code}\n" -X POST "$BASE_URL/api/v1/batches" \
  -F "center_id=center-z" \
  -F "files=@$SAMPLES/reference_first/image_001.dcm" \
  -F "files=@$SAMPLES/invalid/not_a_dicom.dcm"
cat "$WORK/inv.json"; echo
curl -sS "$BASE_URL/api/v1/batches?center_id=center-z"; echo "  (empty list above)"

echo
echo "Interactive docs: open $BASE_URL/docs in a browser."
