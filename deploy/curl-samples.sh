#!/usr/bin/env bash
# Sample API calls against Searchly via the gateway (http://localhost:8080).
# Dev-mode auth: tenant + roles passed via headers (see TenantSecurityFilter).
# In production, these are JWT claims (RS256) — see ADR 0008.

set -e
BASE=${BASE:-http://localhost:8080}
TENANT=${TENANT:-acme}
APP_USER=${APP_USER:-alice}   # seeded users — see README. Roles are resolved from DB.

hdrs=(-H "X-Tenant-Id: $TENANT" -H "X-User-Id: $APP_USER" -H "Content-Type: application/json")

echo "== Health =="
curl -s "$BASE/actuator/health" | jq .

echo "== Pre-seeded data (populated automatically on first boot by DataSeeder) =="
echo "-- Search 'revenue' on acme --"
curl -s "$BASE/api/v1/search?q=revenue&tenant=acme" "${hdrs[@]}" | jq '{total, titles:[.hits[].title]}'
echo "-- Faceted search 'audit' on umbrella --"
curl -s "$BASE/api/v1/search?q=audit&tenant=umbrella&facets=tags&facets=author" \
  -H "X-Tenant-Id: umbrella" -H "X-User-Id: judy" | jq '{total, facets, titles:[.hits[].title]}'

echo "== Index a new document =="
DOC=$(curl -s -X POST "$BASE/api/v1/documents" "${hdrs[@]}" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{
        "title":"Q4 Revenue Report",
        "content":"Revenue grew 23% YoY driven by strong enterprise sales in EMEA and APAC.",
        "metadata":{"author":"alice","tags":["finance","2025"]}
      }')
echo "$DOC" | jq .
ID=$(echo "$DOC" | jq -r .id)

echo "== Wait for async indexing =="
sleep 2

echo "== Search =="
curl -s "$BASE/api/v1/search?q=revenue&tenant=$TENANT&fuzzy=true&highlight=true" "${hdrs[@]}" | jq .

echo "== Get by id =="
curl -s "$BASE/api/v1/documents/$ID" "${hdrs[@]}" | jq .

echo "== Delete =="
curl -s -X DELETE -o /dev/null -w "HTTP %{http_code}\n" "$BASE/api/v1/documents/$ID" "${hdrs[@]}"
