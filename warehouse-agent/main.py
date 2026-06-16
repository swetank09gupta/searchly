"""
Warehouse Agent Service — FastAPI

PRIMARY ENDPOINT (this is what the search-api calls):
  POST /api/v1/chat
    { "message": "why is my robot not coming?", "session_id": "...",
      "customer": "samsclub atl",   ← fuzzy, auto-resolved
      "env": "prod",                ← optional, extracted from message if absent
      "product": "pick-assist" }
    →
    { "session_id": "...",          ← keep and pass back for multi-turn
      "answer": "...",
      "resolved_customer": "sams-club-atlanta",
      "resolved_env": "prod",
      "lifecycle_stage": "prod",
      "needs_clarification": false  ← if true, answer IS a question for the user }

CUSTOMER MANAGEMENT (optional — the chat auto-registers, but ops can use these):
  GET    /api/v1/customers
  POST   /api/v1/customers                             register manually
  GET    /api/v1/customers/{id}
  PATCH  /api/v1/customers/{id}
  DELETE /api/v1/customers/{id}
  POST   /api/v1/customers/{id}/environments/{env}     add cluster env
  DELETE /api/v1/customers/{id}/environments/{env}

HEALTH:
  GET /health
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path as FilePath
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Path, Request
import json as _json

from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field

from agent import run_agent
from auth import AuthDB, AUTH_ENABLED
from chat_handler import ChatHandler
from customer_registry import CustomerRegistry, LIFECYCLE_ORDER, lifecycle_label
from eval_scheduler import EvalScheduler
from products_config import load as load_products, ids as product_ids, product_menu
from session import store as session_store

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────────────────────────
OLLAMA_URL      = os.getenv("OLLAMA_URL",      "http://ollama:11434")
OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL",    "llama3.2:3b")
SEARCHLY_URL    = os.getenv("SEARCHLY_URL",    "http://gateway:8080")
SEARCHLY_TENANT = os.getenv("SEARCHLY_TENANT", "greyorange")
CUSTOMERS_YML   = os.getenv("CUSTOMERS_YML",   "/app/customers.yml")
CUSTOMERS_DB    = os.getenv("CUSTOMERS_DB",    "/app/data/customers_db.json")
AUTH_DB_PATH    = os.getenv("AUTH_DB",         "/app/data/auth_db.json")
PRODUCTS_YML    = os.getenv("PRODUCTS_YML",    "/app/products.yml")
EVAL_DATASET    = os.getenv("EVAL_DATASET",    "/app/eval_dataset.json")

registry:  CustomerRegistry
chat:      ChatHandler
auth_db:   AuthDB
eval_sched: EvalScheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    global registry, chat, auth_db, eval_sched
    load_products(PRODUCTS_YML)   # must be first — registry validation depends on it
    registry = CustomerRegistry(db_path=CUSTOMERS_DB)
    if os.path.exists(CUSTOMERS_YML):
        registry.import_yaml(CUSTOMERS_YML)
    auth_db = AuthDB(db_path=AUTH_DB_PATH)
    chat = ChatHandler(
        registry        = registry,
        session_store   = session_store,
        ollama_url      = OLLAMA_URL,
        ollama_model    = OLLAMA_MODEL,
        searchly_url    = SEARCHLY_URL,
        searchly_tenant = SEARCHLY_TENANT,
    )
    eval_sched = EvalScheduler(
        agent_url    = f"http://localhost:{os.getenv('PORT', '8084')}",
        tenant       = SEARCHLY_TENANT,
        dataset_path = EVAL_DATASET,
        ollama_url   = OLLAMA_URL,
        ollama_model = OLLAMA_MODEL,
    )
    eval_sched.start()
    log.info("Warehouse agent ready  ollama=%s  model=%s  customers=%d  auth=%s",
             OLLAMA_URL, OLLAMA_MODEL, len(registry.list_customers()),
             "on" if AUTH_ENABLED else "off")
    yield
    eval_sched.stop()


# ─── Auth helper ──────────────────────────────────────────────────────────────

def _require_key(request: Request) -> dict:
    """
    Extract and validate API key from X-API-Key header or ?key= query param.
    If AUTH_ENABLED=false, always passes (returns anonymous record).
    Raises 401 on invalid key.
    """
    raw = (
        request.headers.get("x-api-key")
        or request.query_params.get("key")
        or ""
    )
    record = auth_db.validate(raw)
    if record is None:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API key. Pass X-API-Key header.",
        )
    return record


app = FastAPI(
    title="GreyOrange Warehouse Intelligence Agent",
    version="3.0.0",
    description=(
        "Conversational warehouse intelligence — self-registers customers and envs "
        "through chat, resolves 'samsclub atl' → 'sams-club-atlanta' automatically, "
        "queries live k8s clusters, and answers from code + Jira + Confluence."
    ),
    lifespan=lifespan,
)


# ─── Pydantic models ───────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    message:    str
    session_id: str | None = None
    customer:   str | None = Field(None, description="Customer hint — fuzzy matched automatically")
    env:        str | None = Field(None, description="dev | testing | staging | prod (extracted from message if absent)")
    product:    str | None = None


class ChatResponse(BaseModel):
    session_id:          str
    answer:              str
    resolved_customer:   str | None
    resolved_env:        str | None
    lifecycle_stage:     str | None
    lifecycle_label:     str | None
    has_live_data:       bool
    needs_clarification: bool
    tools_called:        list[str]
    is_operational:      bool


class CreateCustomerRequest(BaseModel):
    id:              str
    name:            str
    products:        list[str] = Field(..., description="Must be valid product IDs from GET /api/v1/products")
    lifecycle_stage: str = "solution"
    notes:           str = ""
    aliases:         list[str] = Field(default_factory=list)

    @property
    def validated_products(self) -> list[str]:
        from products_config import validate_products
        valid, unknown = validate_products(self.products)
        if unknown:
            raise ValueError(f"Unknown products: {unknown}. Valid options: {product_ids()}")
        return valid


class UpdateCustomerRequest(BaseModel):
    name:            str | None = None
    notes:           str | None = None
    lifecycle_stage: str | None = None
    products:        list[str] | None = None
    aliases:         list[str] | None = None


class UpsertEnvRequest(BaseModel):
    # ── Kubernetes cluster access ──────────────────────────────────────────────
    k8s_bastion:   str = Field("", description="SSH bastion for kubectl, e.g. user@10.0.1.5")
    k8s_context:   str = Field("", description="kubectl context name")
    k8s_namespace: str = Field("default", description="Kubernetes namespace where customer pods run")
    pod_map:       dict[str, str] = Field(default_factory=dict)

    # ── Elasticsearch — Mode A: bastion-kubectl (zero credential storage) ──────
    # Default for all GreyOrange ECK deployments. The agent fetches the ES password
    # at runtime from the k8s Secret via bastion SSH, then execs curl inside a
    # Filebeat pod. No password stored anywhere in config.
    elastic_k8s_ns:     str = Field(
        "elastic-system",
        description="k8s namespace where the ECK ES cluster runs (default: elastic-system)",
    )
    elastic_k8s_secret: str = Field(
        "gm-elasticsearch-es-elastic-user",
        description="k8s Secret name containing the ES elastic user password",
    )
    elastic_k8s_svc:    str = Field(
        "gm-elasticsearch-es-http",
        description="k8s Service name for the ES cluster (ClusterIP, accessed from inside pods)",
    )
    elastic_index:      str = Field(
        "filebeat-*",
        description="ES index pattern (default: filebeat-* for ECK + Filebeat setups)",
    )
    elastic_fields:     dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Override default ES field names if Logstash config differs. "
            "Keys: timestamp, message, level, namespace, pod, container, app_label. "
            "Example: {\"namespace\": \"kubernetes.namespace\", \"level\": \"log_level\"}"
        ),
    )

    # ── Elasticsearch — Mode B: direct HTTP (external URL + credentials) ───────
    # Only needed when ES is exposed externally via ingress + API key or password.
    # Most GreyOrange envs don't need this — Mode A (above) works automatically.
    elastic_url:       str = Field(
        "",
        description=(
            "External Elasticsearch API endpoint. "
            "Leave empty to use bastion-kubectl mode (recommended). "
            "Example: https://pickassistsim-es.greymatter.greyorange.com"
        ),
    )
    elastic_api_key:   str = Field("", description="ES API key (Mode B only)")
    elastic_user:      str = Field("", description="ES basic-auth username (Mode B only)")
    elastic_password:  str = Field("", description="ES basic-auth password (Mode B only)")
    elastic_verify_ssl: bool = Field(True, description="Set false for self-signed certs (Mode B)")


# ─── PRIMARY: Chat endpoint ───────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def serve_ui():
    """Serve the chat UI — open this in your browser."""
    ui_path = FilePath(__file__).parent / "static" / "chat.html"
    if not ui_path.exists():
        raise HTTPException(status_code=404, detail="Chat UI not found — check static/chat.html")
    return HTMLResponse(content=ui_path.read_text())


@app.get("/api/v1/config", summary="Client configuration")
async def get_config(request: Request) -> dict:
    """
    Returns server-side feature flags the UI needs.
    If AUTH_ENABLED=true, also validates the key from this request.
    """
    if AUTH_ENABLED:
        # Validate key if auth is on — 401 on bad key
        _require_key(request)
    return {
        "auth_enabled":  AUTH_ENABLED,
        "ollama_model":  OLLAMA_MODEL,
        "customers":     len(registry.list_customers()),
    }


@app.get("/api/v1/products", summary="List valid product IDs and descriptions")
async def list_products() -> dict:
    """
    Returns the canonical product list (loaded from products.yml).
    Use these IDs when registering customers — no other values are accepted.
    """
    from products_config import PRODUCTS
    return {
        "products": [
            {"id": pid, "description": desc}
            for pid, desc in PRODUCTS.items()
        ],
        "usage": "Pass product IDs when asked which products a customer uses.",
    }


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "model": OLLAMA_MODEL,
            "customers": len(registry.list_customers())}


@app.post("/api/v1/chat", response_model=ChatResponse)
async def chat_endpoint(req: ChatRequest, request: Request) -> ChatResponse:
    """
    The primary endpoint.  Send any warehouse question in natural language.

    - Customer and env are extracted from the message automatically.
    - Unknown customers are registered through the conversation.
    - Pass session_id back with each subsequent message for multi-turn context.
    - If needs_clarification=true, the answer field IS the question — display it
      to the user and send their reply back with the same session_id.

    Tenant isolation:
    - When AUTH_ENABLED=true, your API key determines which customers you can
      access.  If the resolved customer is not in your allowed list you get a
      polite refusal, not a 403 — the UI needs to handle the response gracefully.
    """
    if not req.message or not req.message.strip():
        raise HTTPException(status_code=400, detail="message is required")

    key_record = _require_key(request)

    try:
        result = await chat.handle(
            message       = req.message.strip(),
            session_id    = req.session_id,
            customer_hint = req.customer,
            env_hint      = req.env,
            product_hint  = req.product,
        )
    except Exception as e:
        log.exception("Chat handler failed")
        raise HTTPException(status_code=500, detail=str(e))

    # ── Tenant isolation check ────────────────────────────────────────────────
    resolved_customer = result.get("resolved_customer")
    if resolved_customer and not auth_db.is_customer_allowed(key_record, resolved_customer):
        log.warning(
            "Tenant isolation: key %r denied access to customer %r",
            key_record.get("name"), resolved_customer,
        )
        # Return a polite refusal (not a 403) so multi-turn still works
        result["answer"] = (
            "I'm sorry — your current credentials don't have access to "
            f"**{resolved_customer}**'s data.\n\n"
            "Please contact your GreyOrange administrator to request access, "
            "or ask about a different customer."
        )
        result["resolved_customer"] = None
        result["resolved_env"]      = None
        result["lifecycle_stage"]   = None
        result["lifecycle_label"]   = None
        result["has_live_data"]     = False
        result["needs_clarification"] = False

    return ChatResponse(**{k: result.get(k) for k in ChatResponse.model_fields})


@app.post("/api/v1/chat/stream")
async def chat_stream_endpoint(req: ChatRequest, request: Request):
    """
    Streaming version of /api/v1/chat.  Returns text/event-stream SSE:
      data: {"type":"status","message":"Searching knowledge base..."}
      data: {"type":"token","content":"Based "}
      data: {"type":"done","session_id":"...","answer":"...",...}
    """
    if not req.message or not req.message.strip():
        raise HTTPException(status_code=400, detail="message is required")

    key_record = _require_key(request)

    async def event_generator():
        try:
            async for event in chat.handle_stream(
                message       = req.message.strip(),
                session_id    = req.session_id,
                customer_hint = req.customer,
                env_hint      = req.env,
                product_hint  = req.product,
            ):
                if event.get("type") == "done":
                    resolved_customer = event.get("resolved_customer")
                    if resolved_customer and not auth_db.is_customer_allowed(key_record, resolved_customer):
                        log.warning("Tenant isolation: key %r denied customer %r",
                                    key_record.get("name"), resolved_customer)
                        event = {
                            **event,
                            "answer": (
                                "I'm sorry — your current credentials don't have access to "
                                f"**{resolved_customer}**'s data.\n\n"
                                "Please contact your GreyOrange administrator to request access."
                            ),
                            "resolved_customer": None,
                            "resolved_env":      None,
                            "lifecycle_stage":   None,
                            "lifecycle_label":   None,
                            "has_live_data":     False,
                        }
                yield f"data: {_json.dumps(event)}\n\n"
        except Exception as e:
            log.exception("Stream handler failed")
            yield f"data: {_json.dumps({'type': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ─── Customer management (ops / admin use) ────────────────────────────────────

@app.get("/api/v1/customers")
async def list_customers() -> list[dict]:
    cs = registry.list_customers()
    for c in cs:
        c["lifecycle_label"] = lifecycle_label(c.get("lifecycle_stage", "solution"))
    return cs


@app.post("/api/v1/customers", status_code=201)
async def create_customer(req: CreateCustomerRequest) -> dict:
    try:
        valid_products = req.validated_products
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    try:
        c = registry.create(
            customer_id     = req.id,
            name            = req.name,
            products        = valid_products,
            lifecycle_stage = req.lifecycle_stage,
            notes           = req.notes,
        )
        if req.aliases:
            c = registry.update(req.id, aliases=req.aliases)
        c["lifecycle_label"] = lifecycle_label(c["lifecycle_stage"])
        return c
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))


@app.get("/api/v1/customers/{customer_id}")
async def get_customer(customer_id: str = Path(...)) -> dict:
    c = registry.get(customer_id)
    if not c:
        raise HTTPException(status_code=404, detail=f"Customer not found: {customer_id!r}")
    c["lifecycle_label"] = lifecycle_label(c.get("lifecycle_stage", "solution"))
    return c


@app.patch("/api/v1/customers/{customer_id}")
async def update_customer(customer_id: str, req: UpdateCustomerRequest) -> dict:
    updates = {k: v for k, v in req.model_dump().items() if v is not None}
    try:
        c = registry.update(customer_id, **updates)
        c["lifecycle_label"] = lifecycle_label(c["lifecycle_stage"])
        return c
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.delete("/api/v1/customers/{customer_id}", status_code=204)
async def delete_customer(customer_id: str):
    try:
        registry.delete(customer_id)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/api/v1/customers/{customer_id}/environments")
async def list_environments(customer_id: str) -> dict:
    c = registry.get(customer_id)
    if not c:
        raise HTTPException(status_code=404, detail=f"Customer not found: {customer_id!r}")
    envs = c.get("environments", {})
    return {
        "customer_id":     customer_id,
        "lifecycle_stage": c["lifecycle_stage"],
        "lifecycle_label": lifecycle_label(c["lifecycle_stage"]),
        "configured_envs": list(envs.keys()),
        "missing_envs":    [e for e in LIFECYCLE_ORDER[1:] if e not in envs],
        "aliases":         c.get("aliases", []),
        "environments":    {
            env: {
                **cfg,
                "has_cluster":   bool(cfg.get("k8s_bastion") or cfg.get("k8s_context")),
                "has_elastic":   bool(cfg.get("elastic_url")),
                "elastic_index": cfg.get("elastic_index", "logstash-*"),
            }
            for env, cfg in envs.items()
        },
    }


@app.post("/api/v1/customers/{customer_id}/environments/{env}")
async def upsert_environment(customer_id: str, env: str,
                              req: UpsertEnvRequest) -> dict:
    if env not in LIFECYCLE_ORDER[1:]:
        raise HTTPException(status_code=400,
                            detail=f"env must be one of {LIFECYCLE_ORDER[1:]}")
    try:
        c = registry.upsert_environment(
            customer_id         = customer_id,
            env                 = env,
            k8s_bastion         = req.k8s_bastion,
            k8s_context         = req.k8s_context,
            k8s_namespace       = req.k8s_namespace,
            pod_map             = req.pod_map,
            elastic_k8s_ns      = req.elastic_k8s_ns,
            elastic_k8s_secret  = req.elastic_k8s_secret,
            elastic_k8s_svc     = req.elastic_k8s_svc,
            elastic_index       = req.elastic_index,
            elastic_fields      = req.elastic_fields or None,
            elastic_url         = req.elastic_url,
            elastic_api_key     = req.elastic_api_key,
            elastic_user        = req.elastic_user,
            elastic_password    = req.elastic_password,
            elastic_verify_ssl  = req.elastic_verify_ssl,
        )
        c["lifecycle_label"] = lifecycle_label(c["lifecycle_stage"])
        return c
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.delete("/api/v1/customers/{customer_id}/environments/{env}", status_code=204)
async def remove_environment(customer_id: str, env: str):
    try:
        registry.remove_environment(customer_id, env)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


# ─── Auth key management (admin only) ────────────────────────────────────────

class CreateKeyRequest(BaseModel):
    name:              str
    allowed_customers: list[str] = Field(
        default=["*"],
        description=(
            'List of customer IDs this key can access. '
            'Use ["*"] for full access (admin). '
            'Use ["sams-club-atlanta"] to restrict to one customer.'
        ),
    )
    is_admin: bool = False


@app.get("/api/v1/auth/keys",
         summary="List API keys (admin only, keys are masked)")
async def list_auth_keys(request: Request) -> list[dict]:
    """Returns all keys with the key value masked. Requires an admin key."""
    key_record = _require_key(request)
    if not key_record.get("is_admin"):
        raise HTTPException(status_code=403, detail="Admin key required")
    return auth_db.list_keys()


@app.post("/api/v1/auth/keys", status_code=201,
          summary="Create API key (admin only)")
async def create_auth_key(req: CreateKeyRequest,
                          request: Request) -> dict:
    """
    Create a new API key.

    The key value is returned once in the response — store it immediately.
    It cannot be recovered after this call.

    Examples:
      # Full access (GreyOrange admin / solution engineer)
      {"name": "Alice (Solution)", "allowed_customers": ["*"]}

      # Customer-scoped (lock to one customer)
      {"name": "Sam's Club ATL ops", "allowed_customers": ["sams-club-atlanta"]}

      # Multi-customer (e.g. regional manager)
      {"name": "APAC manager", "allowed_customers": ["sams-club-atlanta", "sams-club-canada"]}
    """
    key_record = _require_key(request)
    if not key_record.get("is_admin"):
        raise HTTPException(status_code=403, detail="Admin key required")

    # Validate customer IDs (unless ["*"])
    if req.allowed_customers != ["*"]:
        unknown = [cid for cid in req.allowed_customers
                   if not registry.get(cid)]
        if unknown:
            raise HTTPException(
                status_code=422,
                detail=f"Unknown customer IDs: {unknown}. "
                       f"Register them first via POST /api/v1/customers",
            )

    new_record = auth_db.create_key(
        name=req.name,
        allowed_customers=req.allowed_customers,
        is_admin=req.is_admin,
    )
    log.info("API key created: name=%r customers=%r by admin=%r",
             req.name, req.allowed_customers, key_record.get("name"))
    return new_record  # full key visible here only


@app.delete("/api/v1/auth/keys/{key_prefix}", status_code=204,
            summary="Delete API key by prefix (admin only)")
async def delete_auth_key(key_prefix: str, request: Request):
    """
    Delete a key by its first 16 characters (shown in GET /api/v1/auth/keys).
    Cannot delete the last admin key.
    """
    key_record = _require_key(request)
    if not key_record.get("is_admin"):
        raise HTTPException(status_code=403, detail="Admin key required")
    try:
        auth_db.delete_key(key_prefix)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
