#!/bin/sh
set -e

MODEL="${OLLAMA_MODEL:-llama3.2:3b}"

# Start Ollama server in the background
ollama serve &
SERVER_PID=$!

# Wait for the server to be ready
echo "Waiting for Ollama server to start..."
until curl -sf http://localhost:11434/ > /dev/null 2>&1; do
  sleep 1
done
echo "Ollama server ready."

# Pull model only if not already present in the volume
if ollama list | grep -q "^${MODEL}"; then
  echo "Model ${MODEL} already present, skipping pull."
else
  echo "Pulling model ${MODEL} (first-time download, may take a few minutes)..."
  ollama pull "${MODEL}"
  echo "Model ${MODEL} ready."
fi

# Hand off to the server process
wait $SERVER_PID
