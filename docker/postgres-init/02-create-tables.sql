-- Crear tablas para almacenamiento de documentos y vectores

-- Tabla para documentos
CREATE TABLE IF NOT EXISTS rag.documents (
    id SERIAL PRIMARY KEY,
    document_id TEXT UNIQUE NOT NULL,
    source TEXT,
    title TEXT,
    author TEXT,
    date TIMESTAMP,
    content_type TEXT,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabla para chunks/fragmentos
CREATE TABLE IF NOT EXISTS rag.chunks (
    id SERIAL PRIMARY KEY,
    chunk_id TEXT UNIQUE NOT NULL,
    document_id TEXT, -- Haciendo opcional el ID del documento
    content TEXT NOT NULL,
    embedding VECTOR(768), -- Ajustar según dimensiones de los embeddings
    metadata JSONB,
    source TEXT,
    url TEXT,
    title TEXT,
    date TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    -- Eliminada la FOREIGN KEY para permitir chunks sin referencia a documentos
);

-- Crear índice para búsqueda rápida por document_id
CREATE INDEX IF NOT EXISTS idx_chunks_document_id ON rag.chunks(document_id);

-- Crear índice para búsqueda por texto (usando GIN para búsqueda full-text)
CREATE INDEX IF NOT EXISTS idx_chunks_text ON rag.chunks USING GIN (to_tsvector('spanish', text));

-- Crear índice HNSW para búsqueda vectorial rápida
-- Índice aproximado optimizado para alta velocidad y buen recall
CREATE INDEX IF NOT EXISTS idx_chunks_embedding ON rag.chunks USING hnsw (embedding vector_cosine_ops)
WITH (ef_construction = 128, m = 16);

-- Tabla para historial de consultas (útil para análisis y mejoras)
CREATE TABLE IF NOT EXISTS rag.query_history (
    id SERIAL PRIMARY KEY,
    query_text TEXT NOT NULL,
    results JSONB,
    feedback JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabla para caché de respuestas (para consultas repetidas)
CREATE TABLE IF NOT EXISTS rag.response_cache (
    id SERIAL PRIMARY KEY,
    query_hash TEXT UNIQUE NOT NULL,
    query_text TEXT NOT NULL,
    response JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP WITH TIME ZONE
);

-- Comentario informativo para administradores
COMMENT ON SCHEMA rag IS 'Esquema para el sistema RAG (Retrieval-Augmented Generation) de SUNASS';
COMMENT ON TABLE rag.documents IS 'Almacena metadatos de documentos procesados';
COMMENT ON TABLE rag.chunks IS 'Almacena fragmentos de texto con sus embeddings vectoriales';
COMMENT ON TABLE rag.query_history IS 'Historial de consultas y resultados para análisis';
COMMENT ON TABLE rag.response_cache IS 'Caché de respuestas para optimizar consultas repetidas';
