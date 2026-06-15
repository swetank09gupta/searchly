from fastapi import FastAPI
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer, CrossEncoder
import uvicorn

app = FastAPI(title="Searchly Embedding Service")

# BGE-small-en-v1.5: same 384-dim output as all-MiniLM-L6-v2 so no index remapping needed.
# BGE requires query-time prefix for retrieval tasks; passage encoding uses no prefix.
MODEL_NAME = "BAAI/bge-small-en-v1.5"
DIMENSION = 384
QUERY_PREFIX = "Represent this sentence: "

model = SentenceTransformer(MODEL_NAME)

RERANKER_MODEL = "BAAI/bge-reranker-base"
reranker = CrossEncoder(RERANKER_MODEL)


class EmbedRequest(BaseModel):
    texts: list[str]
    is_query: bool = False  # True when embedding a search query; False for passage indexing


class EmbedResponse(BaseModel):
    vectors: list[list[float]]
    dimension: int


@app.post("/embed", response_model=EmbedResponse)
def embed(req: EmbedRequest):
    texts = [QUERY_PREFIX + t for t in req.texts] if req.is_query else req.texts
    vectors = model.encode(texts, normalize_embeddings=True).tolist()
    return EmbedResponse(vectors=vectors, dimension=DIMENSION)


class RerankRequest(BaseModel):
    query: str
    passages: list[str]


class RerankResponse(BaseModel):
    scores: list[float]  # relevance score per passage, same order as input


@app.post("/rerank", response_model=RerankResponse)
def rerank(req: RerankRequest):
    pairs = [[req.query, p] for p in req.passages]
    scores = reranker.predict(pairs).tolist()
    return RerankResponse(scores=scores)


@app.get("/health")
def health():
    return {"status": "ok", "model": MODEL_NAME, "reranker": RERANKER_MODEL, "dimension": DIMENSION}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8083)
