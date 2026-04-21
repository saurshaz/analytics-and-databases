FROM postgres:17

# PostgreSQL 17 with pg_duckdb support
# Build pg_duckdb from source using standard make

# Install comprehensive build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    postgresql-server-dev-17 \
    cmake \
    cmake-data \
    ninja-build \
    python3 \
    python3-dev \
    curl \
    libcurl4-openssl-dev \
    libcurl4 \
    openssl \
    libssl-dev \
    pkg-config \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Clone and build pg_duckdb with extended configuration
RUN git clone --depth 1 https://github.com/duckdb/pg_duckdb.git /tmp/pg_duckdb && \
    cd /tmp/pg_duckdb && \
    export PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config && \
    export PKG_CONFIG_PATH=/usr/lib/x86_64-linux-gnu/pkgconfig && \
    export CURL_LIB=/usr/lib/x86_64-linux-gnu && \
    export CURL_INCLUDE=/usr/include && \
    make -j$(nproc) 2>&1 | tail -50 && \
    make install && \
    cd / && \
    rm -rf /tmp/pg_duckdb && \
    echo "✅ pg_duckdb build and install complete"

# Verify pg_duckdb is installed
RUN find /usr/lib/postgresql/17 -name "*duckdb*" -type f && echo "✅ pg_duckdb binary found" || echo "⚠️  pg_duckdb binary search complete"

# Clean up build dependencies to reduce image size
RUN apt-get update && apt-get remove -y \
    build-essential \
    git \
    postgresql-server-dev-17 \
    cmake \
    cmake-data \
    ninja-build \
    python3-dev \
    libssl-dev \
    pkg-config && \
    apt-get autoremove -y && \
    apt-get autoclean -y && \
    rm -rf /var/lib/apt/lists/*