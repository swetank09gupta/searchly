#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
#  start.sh — GreyOrange Warehouse Intelligence — one command to start all
# ─────────────────────────────────────────────────────────────────────────────
# Usage:
#   ./start.sh              # start everything, sync runs automatically
#   ./start.sh --rebuild    # force Docker image rebuild (after code changes)
#   ./start.sh --stop       # stop all containers (data is preserved)
#   ./start.sh --status     # show container health + indexed doc counts
#   ./start.sh --logs       # stream sync-cron logs (Ctrl+C to exit)
#   ./start.sh --force-sync # manual full sync right now (no waiting for schedule)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/deploy/docker-compose.yml"
ENV_FILE="$SCRIPT_DIR/.env"

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'

info()  { echo -e "${BLUE}[searchly]${NC} $*"; }
ok()    { echo -e "${GREEN}[✓]${NC} $*"; }
warn()  { echo -e "${YELLOW}[!]${NC} $*"; }
die()   { echo -e "${RED}[✗]${NC} $*" >&2; exit 1; }
header(){ echo -e "\n${BOLD}$*${NC}"; }

# ── Args ──────────────────────────────────────────────────────────────────────
ACTION="start"
REBUILD=false

for arg in "$@"; do
  case "$arg" in
    --rebuild)    REBUILD=true  ;;
    --stop)       ACTION="stop" ;;
    --status)     ACTION="status" ;;
    --logs)       ACTION="logs" ;;
    --force-sync) ACTION="force-sync" ;;
    --help|-h)    ACTION="help" ;;
    *) die "Unknown argument: $arg  (try --help)" ;;
  esac
done

# ── Help ──────────────────────────────────────────────────────────────────────
if [[ "$ACTION" == "help" ]]; then
  cat <<'EOF'

  GreyOrange Warehouse Intelligence — start.sh

  ./start.sh              Start all services. Sync runs automatically.
  ./start.sh --rebuild    Same, but force-rebuild Docker images first.
  ./start.sh --stop       Stop all containers (data is preserved in volumes).
  ./start.sh --status     Show container health and indexed doc counts.
  ./start.sh --logs       Stream sync progress (Ctrl+C to exit).
  ./start.sh --force-sync Trigger a manual full sync right now.

  Sync schedule (set in .env):
    SYNC_CUSTOMER_INTERVAL_MIN=5     customer logs + deployment every 5 min
    SYNC_FULL_INTERVAL_HOURS=4       Jira + Confluence + repos every 4 h

EOF
  exit 0
fi

# ── Stop ──────────────────────────────────────────────────────────────────────
if [[ "$ACTION" == "stop" ]]; then
  info "Stopping all containers..."
  docker compose -f "$COMPOSE_FILE" down
  ok "Stopped. Volumes preserved — data is safe."
  exit 0
fi

# ── Status ────────────────────────────────────────────────────────────────────
if [[ "$ACTION" == "status" ]]; then
  header "Container health"
  docker compose -f "$COMPOSE_FILE" ps
  echo ""
  header "Recent sync log (last 25 lines)"
  docker compose -f "$COMPOSE_FILE" logs --no-log-prefix --tail=25 sync-cron 2>/dev/null || \
    warn "sync-cron not running."
  echo ""
  header "Indexed documents (OpenSearch)"
  curl -sf "http://localhost:9200/_cat/indices?v&h=index,docs.count,store.size" 2>/dev/null | \
    grep -E "chunks|documents" || warn "OpenSearch not reachable."
  exit 0
fi

# ── Logs ──────────────────────────────────────────────────────────────────────
if [[ "$ACTION" == "logs" ]]; then
  info "Streaming sync-cron logs — Ctrl+C to exit"
  docker compose -f "$COMPOSE_FILE" logs -f --no-log-prefix sync-cron
  exit 0
fi

# ── Force sync ────────────────────────────────────────────────────────────────
if [[ "$ACTION" == "force-sync" ]]; then
  info "Running full sync now (shared knowledge + all customers)..."
  docker compose -f "$COMPOSE_FILE" \
    run --rm -e SEARCHLY_URL=http://search-api:8081 connectors \
    python sync.py --only shared
  docker compose -f "$COMPOSE_FILE" \
    run --rm -e SEARCHLY_URL=http://search-api:8081 connectors \
    python sync.py --only all-customers
  ok "Manual sync complete."
  exit 0
fi


# ═════════════════════════════════════════════════════════════════════════════
#  START
# ═════════════════════════════════════════════════════════════════════════════

header "GreyOrange Warehouse Intelligence"

# ── 1. Prereq checks ─────────────────────────────────────────────────────────
info "Checking prerequisites..."

command -v docker &>/dev/null || die "Docker not installed. https://docs.docker.com/engine/install/"

if ! docker info &>/dev/null; then
  warn "Docker daemon not running — trying to start it..."
  if command -v systemctl &>/dev/null; then
    sudo systemctl start docker
    sleep 3
    docker info &>/dev/null || die "Could not start Docker. Run: sudo systemctl start docker"
    sudo systemctl enable docker --quiet 2>/dev/null || true
    ok "Docker daemon started and enabled"
  else
    die "Docker daemon not running. Start it manually then re-run."
  fi
fi
ok "Docker daemon running"

docker compose version &>/dev/null || die "docker compose v2 not found. https://docs.docker.com/compose/install/"
ok "docker compose v2 available"

# .env at repo root
if [[ ! -f "$ENV_FILE" ]]; then
  if [[ -f "$SCRIPT_DIR/.env.example" ]]; then
    cp "$SCRIPT_DIR/.env.example" "$ENV_FILE"
    warn "Created .env from .env.example — edit it to add your API tokens."
  else
    touch "$ENV_FILE"
    warn ".env not found, created empty one."
  fi
fi

# connectors/.env (Jira / Confluence / GitHub tokens)
CONN_ENV="$SCRIPT_DIR/connectors/.env"
if [[ ! -f "$CONN_ENV" ]]; then
  warn "connectors/.env not found — Jira/Confluence/GitHub sync will be skipped."
  warn "Create it with these keys:"
  warn "  JIRA_URL=https://greyorange.atlassian.net"
  warn "  JIRA_EMAIL=your@email.com"
  warn "  JIRA_TOKEN=<your-atlassian-token>"
  warn "  CONFLUENCE_SPACES=CE,GME,DEV,GSP,AE,GRYMTTR"
  warn "  JIRA_PROJECTS=AES,AE,GM,SRE,PA,PKE"
  warn "  GIT_TOKEN=<your-github-token>"
  touch "$CONN_ENV"
fi

# SSH key
SSH_KEY=$(grep -s '^SSH_KEY_PATH' "$ENV_FILE" | cut -d= -f2- | tr -d '"' | tr -d "'" | xargs || echo "")
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_rsa}"
if [[ ! -f "$SSH_KEY" ]]; then
  warn "SSH key not found at $SSH_KEY — live k8s queries won't work."
  warn "Set SSH_KEY_PATH=/path/to/your/key in .env"
fi

# Disk space
AVAIL_GB=$(df -BG / | awk 'NR==2{gsub("G",""); print $4}')
if [[ "$AVAIL_GB" -lt 30 ]]; then
  warn "Only ${AVAIL_GB}GB free disk — images + data need ~35GB."
  warn "Free space first: docker system prune -f"
fi

echo ""

# ── 2. Build images ───────────────────────────────────────────────────────────
header "Building Docker images"
if $REBUILD; then
  info "Force-rebuild requested (--rebuild)..."
  docker compose -f "$COMPOSE_FILE" build --no-cache
else
  docker compose -f "$COMPOSE_FILE" build
fi
ok "Images built"

# ── 3. Start all services ─────────────────────────────────────────────────────
header "Starting services"
docker compose -f "$COMPOSE_FILE" up -d
ok "All containers started"

# ── 4. Wait for health ────────────────────────────────────────────────────────
header "Waiting for services to become healthy"

wait_for() {
  local label="$1" url="$2" max="${3:-180}"
  local n=0
  printf "  %-30s" "$label"
  until curl -sf "$url" >/dev/null 2>&1; do
    if (( n >= max )); then
      echo -e " ${RED}timed out${NC}"
      warn "Check logs: docker compose -f $COMPOSE_FILE logs $(echo "$label" | tr '[:upper:]' '[:lower:]' | tr ' ' '-')"
      return 1
    fi
    printf "."; sleep 5; n=$((n+5))
  done
  echo -e " ${GREEN}ready${NC} (${n}s)"
}

wait_for "OpenSearch"         "http://localhost:9200/_cluster/health"  120
wait_for "Embedding service"  "http://localhost:8083/health"           90
wait_for "Ollama + model"     "http://localhost:11434/"                300
wait_for "Warehouse agent"    "http://localhost:8084/health"           90

# ── 5. Summary ────────────────────────────────────────────────────────────────
LOCAL_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo "localhost")
OLLAMA_MODEL=$(grep -s '^OLLAMA_MODEL' "$ENV_FILE" | cut -d= -f2- | tr -d '"' | tr -d "'" | xargs || echo "llama3.2:3b")
CUSTOMER_MIN=$(grep -s '^SYNC_CUSTOMER_INTERVAL_MIN' "$ENV_FILE" | cut -d= -f2- | xargs || echo "5")
FULL_HRS=$(grep -s '^SYNC_FULL_INTERVAL_HOURS' "$ENV_FILE" | cut -d= -f2- | xargs || echo "4")

header "Ready"
echo ""
echo -e "  ${BOLD}Chat UI${NC}     →  http://${LOCAL_IP}:8084"
echo -e "  ${BOLD}API docs${NC}    →  http://${LOCAL_IP}:8084/docs"
echo ""
echo -e "  ${BOLD}LLM${NC}         ${OLLAMA_MODEL} on CPU  (~20-40s/query)"
echo ""
echo -e "  ${BOLD}Sync schedule:${NC}"
echo    "    Customer logs/deployment: every ${CUSTOMER_MIN} min (automatic)"
echo    "    Full Jira/Confluence/repos: every ${FULL_HRS} h (first run starting now)"
echo    ""
echo    "  Commands:"
echo    "    ./start.sh --logs         stream sync progress"
echo    "    ./start.sh --status       health + doc counts"
echo    "    ./start.sh --force-sync   sync right now"
echo    "    ./start.sh --stop         stop everything"
echo ""

if docker compose -f "$COMPOSE_FILE" logs warehouse-agent 2>/dev/null | grep -q "GENERATED ADMIN"; then
  warn "Auto-generated admin API key — retrieve it with:"
  echo "    docker compose -f deploy/docker-compose.yml logs warehouse-agent | grep 'GENERATED ADMIN'"
  echo ""
fi

echo -e "${GREEN}Open http://${LOCAL_IP}:8084 in your browser to start chatting.${NC}"
echo ""
