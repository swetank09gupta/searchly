#!/usr/bin/env bash
# Quick local benchmark using Apache Bench (ab).
# Caveat: numbers below come from a single dev laptop with the entire stack co-resident.
# They demonstrate the shape of the latency curve and the effect of caching, not a production claim.

set -e
BASE=${BASE:-http://localhost:8080}
TENANT=${TENANT:-acme}
APP_USER=${APP_USER:-alice}
N=${N:-2000}          # total requests
C=${C:-50}            # concurrency
HDR=(-H "X-Tenant-Id: $TENANT" -H "X-User-Id: $APP_USER")

echo "=== Pre-warm: index 200 docs (synchronous to API, async to OpenSearch) ==="
for i in $(seq 1 200); do
  curl -s -o /dev/null -X POST "$BASE/api/v1/documents" \
    "${HDR[@]}" -H "Content-Type: application/json" \
    -d "{\"title\":\"Bench doc $i\",\"content\":\"performance benchmark document $i with revenue growth keywords\",\"metadata\":{\"author\":\"alice\",\"tags\":[\"bench\",\"$((i % 10))\"]}}"
done
sleep 5

echo
echo "=== Search throughput — cold cache (N=$N, C=$C) ==="
# Use a query string with random suffix to bypass cache for first hit; ab will then re-use it -> cache hit.
ab -l -n "$N" -c "$C" -H "X-Tenant-Id: $TENANT" -H "X-User-Id: $APP_USER" \
   "$BASE/api/v1/search?q=revenue&tenant=$TENANT" 2>&1 | grep -E "Requests per second|Time per request|Percentage|50%|95%|99%|Failed"

echo
echo "=== Search throughput — facets enabled ==="
ab -l -n "$N" -c "$C" -H "X-Tenant-Id: $TENANT" -H "X-User-Id: $APP_USER" \
   "$BASE/api/v1/search?q=revenue&tenant=$TENANT&facets=tags&facets=author" 2>&1 | grep -E "Requests per second|Time per request|Percentage|50%|95%|99%|Failed"

echo
echo "=== Document GET throughput (fixed id, served from Postgres) ==="
DOC_ID=$(curl -s "$BASE/api/v1/search?q=revenue&tenant=$TENANT&size=1" "${HDR[@]}" | jq -r '.hits[0].id')
ab -l -n "$N" -c "$C" -H "X-Tenant-Id: $TENANT" -H "X-User-Id: $APP_USER" \
   "$BASE/api/v1/documents/$DOC_ID" 2>&1 | grep -E "Requests per second|Time per request|Percentage|50%|95%|99%|Failed"

echo
echo "Done. Capture output into docs/BENCHMARKS.md."
