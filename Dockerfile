FROM python:3.12-slim

WORKDIR /app

# Install dependencies and clean up package lists to reduce image size
RUN apt-get update && apt-get install -y \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install UV
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"

# Copy dependencies files
COPY pyproject.toml uv.lock ./

# Install Python dependencies, using uv.lock as is with --frozen
RUN uv sync --frozen

# Copy source code
COPY src/ ./src/
COPY data/ ./data/

# Expose port
EXPOSE 8000

# Command to run the application (production mode)
CMD ["uv", "run", "fastapi", "dev", "src/main.py", "--host", "0.0.0.0", "--port", "8000"]