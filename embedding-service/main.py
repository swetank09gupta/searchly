from fastapi import FastAPI
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
import uvicorn

app = FastAPI(title="Searchly Embedding Service")
model = SentenceTransformer("all-MiniLM-L6-v2")
DIMENSION = 384


class EmbedRequest(BaseModel):
    texts: list[str]


class EmbedResponse(BaseModel):
    vectors: list[list[float]]
    dimension: int


@app.post("/embed", response_model=EmbedResponse)
def embed(req: EmbedRequest):
    vectors = model.encode(req.texts, normalize_embeddings=True).tolist()
    return EmbedResponse(vectors=vectors, dimension=DIMENSION)


@app.get("/health")
def health():
    return {"status": "ok", "model": "all-MiniLM-L6-v2", "dimension": DIMENSION}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8083)
