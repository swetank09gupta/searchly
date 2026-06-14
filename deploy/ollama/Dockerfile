FROM ollama/ollama:latest

# Start the server, pull the model, then keep serving.
# Model is stored in the named volume so it only downloads once.
COPY start.sh /start.sh
RUN chmod +x /start.sh

EXPOSE 11434
ENTRYPOINT ["/start.sh"]
