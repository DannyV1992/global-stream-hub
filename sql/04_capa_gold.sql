-- =============================================================================
-- PARTE 4: CAPA GOLD - MODELO DIMENSIONAL PARA BI Y ANÁLISIS
-- =============================================================================

-- =============================================================================
-- PASO 9: CREACIÓN DE TABLAS DE DIMENSIONES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 9.1. Dimensión: gold.dim_users
-- Dimensión de usuarios con atributos desnormalizados para análisis rápido
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_users (
    user_key           SERIAL PRIMARY KEY,
    user_id            INT NOT NULL UNIQUE,
    source_user_id     VARCHAR(100),
    full_name          VARCHAR(255),
    email              VARCHAR(255),
    country_name       VARCHAR(150),
    country_iso_code   VARCHAR(10),
    region             VARCHAR(100),
    current_plan_name  VARCHAR(100),
    current_plan_price NUMERIC(10,2),
    registration_date  DATE,
    is_active          BOOLEAN,
    last_activity_date DATE,
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE gold.dim_users IS 'Dimensión de usuarios desnormalizada para análisis.';
COMMENT ON COLUMN gold.dim_users.last_activity_date IS 'Fecha de última actividad de streaming del usuario.';

-- -----------------------------------------------------------------------------
-- 9.2. Dimensión: gold.dim_content
-- Dimensión de contenido con géneros y artistas concatenados
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_content (
    content_key            SERIAL PRIMARY KEY,
    content_id             INT NOT NULL UNIQUE,
    source_content_id      VARCHAR(100),
    title                  VARCHAR(500),
    content_type           VARCHAR(50),
    release_year           INT,
    duration_minutes       NUMERIC(10,2),
    description            TEXT,
    genres_list            TEXT,              -- Géneros concatenados
    primary_genre          VARCHAR(200),      -- Género principal
    artists_list           TEXT,              -- Artistas/directores concatenados
    primary_artist         VARCHAR(300),      -- Artista/director principal
    created_at             TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at             TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE gold.dim_content IS 'Dimensión de contenido con atributos desnormalizados.';
COMMENT ON COLUMN gold.dim_content.genres_list IS 'Lista de géneros separados por comas.';
COMMENT ON COLUMN gold.dim_content.artists_list IS 'Lista de artistas/directores separados por comas.';

-- -----------------------------------------------------------------------------
-- 9.3. Dimensión: gold.dim_devices
-- Dimensión de dispositivos
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_devices (
    device_key         SERIAL PRIMARY KEY,
    device_id          INT NOT NULL UNIQUE,
    device_type        VARCHAR(100),
    operating_system   VARCHAR(100),
    device_category    VARCHAR(50),          -- Mobile, Desktop, TV, Tablet
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE gold.dim_devices IS 'Dimensión de dispositivos de reproducción.';
COMMENT ON COLUMN gold.dim_devices.device_category IS 'Categoría agrupada del dispositivo.';

-- -----------------------------------------------------------------------------
-- 9.4. Dimensión: gold.dim_genres
-- Dimensión de géneros con jerarquía
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_genres (
    genre_key          SERIAL PRIMARY KEY,
    genre_id           INT NOT NULL UNIQUE,
    genre_name         VARCHAR(200),
    parent_genre_name  VARCHAR(200),
    genre_hierarchy    VARCHAR(500),         -- Path completo de jerarquía
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE gold.dim_genres IS 'Dimensión de géneros con información jerárquica.';

-- -----------------------------------------------------------------------------
-- 9.5. Dimensión: gold.dim_date
-- Dimensión de fechas para análisis temporal
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key           SERIAL PRIMARY KEY,
    full_date          DATE NOT NULL UNIQUE,
    year               INT,
    quarter            INT,
    month              INT,
    month_name         VARCHAR(20),
    week               INT,
    day_of_month       INT,
    day_of_week        INT,
    day_name           VARCHAR(20),
    is_weekend         BOOLEAN,
    is_holiday         BOOLEAN DEFAULT FALSE,
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE gold.dim_date IS 'Dimensión de calendario para análisis temporal.';

-- -----------------------------------------------------------------------------
-- 9.6. Dimensión: gold.dim_time_of_day
-- Dimensión de hora del día para análisis de patrones horarios
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_time_of_day (
    time_key           SERIAL PRIMARY KEY,
    hour               INT NOT NULL UNIQUE,
    time_period        VARCHAR(50),          -- Madrugada, Mañana, Tarde, Noche
    is_business_hours  BOOLEAN,
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE gold.dim_time_of_day IS 'Dimensión de hora del día para análisis de patrones horarios.';

-- =============================================================================
-- PASO 10: CREACIÓN DE TABLAS DE HECHOS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 10.1. Tabla de Hechos: gold.fact_user_activity
-- Actividad agregada de usuarios por día
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_user_activity (
    activity_key            SERIAL PRIMARY KEY,
    user_key                INT NOT NULL,
    content_key             INT NOT NULL,
    device_key              INT,
    date_key                INT NOT NULL,
    time_key                INT,
    total_sessions          INT DEFAULT 0,
    total_duration_seconds  NUMERIC(18,2) DEFAULT 0,
    total_duration_minutes  NUMERIC(18,2) DEFAULT 0,
    completed_content_count INT DEFAULT 0,
    avg_completion_rate     NUMERIC(5,2) DEFAULT 0,
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_fact_user_activity_user
        FOREIGN KEY (user_key) REFERENCES gold.dim_users(user_key),
    CONSTRAINT fk_fact_user_activity_content
        FOREIGN KEY (content_key) REFERENCES gold.dim_content(content_key),
    CONSTRAINT fk_fact_user_activity_device
        FOREIGN KEY (device_key) REFERENCES gold.dim_devices(device_key),
    CONSTRAINT fk_fact_user_activity_date
        FOREIGN KEY (date_key) REFERENCES gold.dim_date(date_key),
    CONSTRAINT fk_fact_user_activity_time
        FOREIGN KEY (time_key) REFERENCES gold.dim_time_of_day(time_key)
);

COMMENT ON TABLE gold.fact_user_activity IS 'Hechos de actividad de usuarios agregados por día.';
COMMENT ON COLUMN gold.fact_user_activity.completed_content_count IS 'Cantidad de contenidos completados (>90% visto).';

-- -----------------------------------------------------------------------------
-- 10.2. Tabla de Hechos: gold.fact_content_popularity
-- Popularidad de contenido agregada por período
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_content_popularity (
    popularity_key          SERIAL PRIMARY KEY,
    content_key             INT NOT NULL,
    genre_key               INT,
    date_key                INT NOT NULL,
    total_views             INT DEFAULT 0,
    unique_users            INT DEFAULT 0,
    total_watch_time_seconds NUMERIC(18,2) DEFAULT 0,
    total_watch_time_minutes NUMERIC(18,2) DEFAULT 0,
    avg_completion_rate     NUMERIC(5,2) DEFAULT 0,
    total_sessions          INT DEFAULT 0,
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_fact_content_pop_content
        FOREIGN KEY (content_key) REFERENCES gold.dim_content(content_key),
    CONSTRAINT fk_fact_content_pop_genre
        FOREIGN KEY (genre_key) REFERENCES gold.dim_genres(genre_key),
    CONSTRAINT fk_fact_content_pop_date
        FOREIGN KEY (date_key) REFERENCES gold.dim_date(date_key)
);

COMMENT ON TABLE gold.fact_content_popularity IS 'Hechos de popularidad de contenido agregados por día.';

-- -----------------------------------------------------------------------------
-- 10.3. Tabla de Hechos: gold.fact_ad_performance
-- Rendimiento de anuncios publicitarios
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_ad_performance (
    ad_performance_key      SERIAL PRIMARY KEY,
    ad_id                   INT NOT NULL,
    campaign_id             INT,
    content_key             INT,
    date_key                INT NOT NULL,
    total_impressions       INT DEFAULT 0,
    total_clicks            INT DEFAULT 0,
    click_through_rate      NUMERIC(5,2) DEFAULT 0,
    total_ad_time_seconds   NUMERIC(18,2) DEFAULT 0,
    unique_users            INT DEFAULT 0,
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_fact_ad_perf_content
        FOREIGN KEY (content_key) REFERENCES gold.dim_content(content_key),
    CONSTRAINT fk_fact_ad_perf_date
        FOREIGN KEY (date_key) REFERENCES gold.dim_date(date_key)
);

COMMENT ON TABLE gold.fact_ad_performance IS 'Hechos de rendimiento de anuncios publicitarios.';
COMMENT ON COLUMN gold.fact_ad_performance.click_through_rate IS 'Tasa de clicks (CTR) calculada.';

-- =============================================================================
-- PASO 11: POBLACIÓN DE TABLAS DE DIMENSIONES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 11.1. Poblar dim_date (calendario de 2024-2026)
-- -----------------------------------------------------------------------------
INSERT INTO gold.dim_date (full_date, year, quarter, month, month_name, week, day_of_month, day_of_week, day_name, is_weekend)
SELECT 
    date_series::DATE AS full_date,
    EXTRACT(YEAR FROM date_series)::INT AS year,
    EXTRACT(QUARTER FROM date_series)::INT AS quarter,
    EXTRACT(MONTH FROM date_series)::INT AS month,
    TO_CHAR(date_series, 'Month') AS month_name,
    EXTRACT(WEEK FROM date_series)::INT AS week,
    EXTRACT(DAY FROM date_series)::INT AS day_of_month,
    EXTRACT(DOW FROM date_series)::INT AS day_of_week,
    TO_CHAR(date_series, 'Day') AS day_name,
    CASE WHEN EXTRACT(DOW FROM date_series) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM GENERATE_SERIES('2024-01-01'::DATE, '2026-12-31'::DATE, '1 day'::INTERVAL) AS date_series
ON CONFLICT (full_date) DO NOTHING;

-- -----------------------------------------------------------------------------
-- 11.2. Poblar dim_time_of_day (0-23 horas)
-- -----------------------------------------------------------------------------
INSERT INTO gold.dim_time_of_day (hour, time_period, is_business_hours) VALUES
(0, 'Madrugada', FALSE),
(1, 'Madrugada', FALSE),
(2, 'Madrugada', FALSE),
(3, 'Madrugada', FALSE),
(4, 'Madrugada', FALSE),
(5, 'Madrugada', FALSE),
(6, 'Mañana', FALSE),
(7, 'Mañana', FALSE),
(8, 'Mañana', TRUE),
(9, 'Mañana', TRUE),
(10, 'Mañana', TRUE),
(11, 'Mañana', TRUE),
(12, 'Tarde', TRUE),
(13, 'Tarde', TRUE),
(14, 'Tarde', TRUE),
(15, 'Tarde', TRUE),
(16, 'Tarde', TRUE),
(17, 'Tarde', TRUE),
(18, 'Noche', FALSE),
(19, 'Noche', FALSE),
(20, 'Noche', FALSE),
(21, 'Noche', FALSE),
(22, 'Noche', FALSE),
(23, 'Noche', FALSE)
ON CONFLICT (hour) DO NOTHING;

-- -----------------------------------------------------------------------------
-- 11.3. Poblar dim_users desde silver.users
-- -----------------------------------------------------------------------------
INSERT INTO gold.dim_users (
    user_id, source_user_id, full_name, email, 
    country_name, country_iso_code, region,
    current_plan_name, current_plan_price,
    registration_date, is_active, last_activity_date
)
SELECT 
    u.user_id,
    u.source_user_id,
    u.full_name,
    u.email,
    c.country_name,
    c.iso_code AS country_iso_code,
    c.region,
    p.plan_name AS current_plan_name,
    p.monthly_price AS current_plan_price,
    u.registration_ts::DATE AS registration_date,
    u.active AS is_active,
    (SELECT MAX(s.start_ts::DATE) 
     FROM silver.streaming_sessions s 
     WHERE s.user_id = u.user_id) AS last_activity_date
FROM silver.users u
LEFT JOIN silver.countries c ON u.country_id = c.country_id
LEFT JOIN silver.subscription_plans p ON u.current_plan_id = p.plan_id
ON CONFLICT (user_id) DO UPDATE SET
    full_name = EXCLUDED.full_name,
    email = EXCLUDED.email,
    country_name = EXCLUDED.country_name,
    current_plan_name = EXCLUDED.current_plan_name,
    last_activity_date = EXCLUDED.last_activity_date,
    updated_at = CURRENT_TIMESTAMP;

-- -----------------------------------------------------------------------------
-- 11.4. Poblar dim_content desde silver.content
-- -----------------------------------------------------------------------------
INSERT INTO gold.dim_content (
    content_id, source_content_id, title, content_type,
    release_year, duration_minutes, description,
    genres_list, primary_genre,
    artists_list, primary_artist
)
SELECT 
    c.content_id,
    c.source_content_id,
    c.title,
    c.content_type,
    c.release_year,
    c.duration_minutes,
    c.description,
    -- Géneros concatenados
    (SELECT STRING_AGG(g.genre_name, ', ' ORDER BY g.genre_name)
     FROM silver.content_genres cg
     JOIN silver.genres g ON cg.genre_id = g.genre_id
     WHERE cg.content_id = c.content_id) AS genres_list,
    -- Género principal (primero alfabéticamente)
    (SELECT g.genre_name
     FROM silver.content_genres cg
     JOIN silver.genres g ON cg.genre_id = g.genre_id
     WHERE cg.content_id = c.content_id
     ORDER BY g.genre_name
     LIMIT 1) AS primary_genre,
    -- Artistas concatenados
    (SELECT STRING_AGG(a.name, ', ' ORDER BY a.name)
     FROM silver.content_artists ca
     JOIN silver.artists_directors a ON ca.artist_id = a.artist_id
     WHERE ca.content_id = c.content_id) AS artists_list,
    -- Artista principal (primero alfabéticamente)
    (SELECT a.name
     FROM silver.content_artists ca
     JOIN silver.artists_directors a ON ca.artist_id = a.artist_id
     WHERE ca.content_id = c.content_id
     ORDER BY a.name
     LIMIT 1) AS primary_artist
FROM silver.content c
ON CONFLICT (content_id) DO UPDATE SET
    title = EXCLUDED.title,
    genres_list = EXCLUDED.genres_list,
    artists_list = EXCLUDED.artists_list,
    updated_at = CURRENT_TIMESTAMP;

-- -----------------------------------------------------------------------------
-- 11.5. Poblar dim_devices desde silver.devices
-- -----------------------------------------------------------------------------
INSERT INTO gold.dim_devices (device_id, device_type, operating_system, device_category)
SELECT 
    d.device_id,
    d.device_type,
    d.operating_system,
    CASE 
        WHEN LOWER(d.device_type) LIKE '%mobile%' OR LOWER(d.device_type) LIKE '%phone%' THEN 'Mobile'
        WHEN LOWER(d.device_type) LIKE '%tablet%' OR LOWER(d.device_type) LIKE '%ipad%' THEN 'Tablet'
        WHEN LOWER(d.device_type) LIKE '%tv%' OR LOWER(d.device_type) LIKE '%smart%' THEN 'TV'
        WHEN LOWER(d.device_type) LIKE '%web%' OR LOWER(d.device_type) LIKE '%browser%' THEN 'Desktop'
        ELSE 'Other'
    END AS device_category
FROM silver.devices d
ON CONFLICT (device_id) DO UPDATE SET
    device_type = EXCLUDED.device_type,
    device_category = EXCLUDED.device_category;

-- -----------------------------------------------------------------------------
-- 11.6. Poblar dim_genres desde silver.genres
-- -----------------------------------------------------------------------------
INSERT INTO gold.dim_genres (genre_id, genre_name, parent_genre_name, genre_hierarchy)
SELECT 
    g.genre_id,
    g.genre_name,
    pg.genre_name AS parent_genre_name,
    CASE 
        WHEN pg.genre_name IS NOT NULL THEN pg.genre_name || ' > ' || g.genre_name
        ELSE g.genre_name
    END AS genre_hierarchy
FROM silver.genres g
LEFT JOIN silver.genres pg ON g.parent_genre_id = pg.genre_id
ON CONFLICT (genre_id) DO UPDATE SET
    genre_name = EXCLUDED.genre_name,
    parent_genre_name = EXCLUDED.parent_genre_name,
    genre_hierarchy = EXCLUDED.genre_hierarchy;

-- =============================================================================
-- PASO 12: POBLACIÓN DE TABLAS DE HECHOS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 12.1. Poblar fact_user_activity desde silver.streaming_sessions
-- Agregación por usuario, contenido, dispositivo y fecha
-- -----------------------------------------------------------------------------
INSERT INTO gold.fact_user_activity (
    user_key, content_key, device_key, date_key, time_key,
    total_sessions, total_duration_seconds, total_duration_minutes,
    completed_content_count, avg_completion_rate
)
SELECT 
    du.user_key,
    dc.content_key,
    dd.device_key,
    ddt.date_key,
    dt.time_key,
    COUNT(s.session_id) AS total_sessions,
    SUM(s.duration_seconds) AS total_duration_seconds,
    SUM(s.duration_seconds) / 60.0 AS total_duration_minutes,
    SUM(CASE WHEN s.watched_pct >= 90 THEN 1 ELSE 0 END) AS completed_content_count,
    AVG(s.watched_pct) AS avg_completion_rate
FROM silver.streaming_sessions s
JOIN gold.dim_users du ON s.user_id = du.user_id
JOIN gold.dim_content dc ON s.content_id = dc.content_id
LEFT JOIN gold.dim_devices dd ON s.device_id = dd.device_id
JOIN gold.dim_date ddt ON s.start_ts::DATE = ddt.full_date
LEFT JOIN gold.dim_time_of_day dt ON EXTRACT(HOUR FROM s.start_ts)::INT = dt.hour
GROUP BY du.user_key, dc.content_key, dd.device_key, ddt.date_key, dt.time_key
ON CONFLICT DO NOTHING;

-- -----------------------------------------------------------------------------
-- 12.2. Poblar fact_content_popularity desde silver.streaming_sessions
-- Agregación por contenido, género y fecha
-- -----------------------------------------------------------------------------
INSERT INTO gold.fact_content_popularity (
    content_key, genre_key, date_key,
    total_views, unique_users, total_watch_time_seconds,
    total_watch_time_minutes, avg_completion_rate, total_sessions
)
SELECT 
    dc.content_key,
    dg.genre_key,
    ddt.date_key,
    COUNT(s.session_id) AS total_views,
    COUNT(DISTINCT s.user_id) AS unique_users,
    SUM(s.duration_seconds) AS total_watch_time_seconds,
    SUM(s.duration_seconds) / 60.0 AS total_watch_time_minutes,
    AVG(s.watched_pct) AS avg_completion_rate,
    COUNT(s.session_id) AS total_sessions
FROM silver.streaming_sessions s
JOIN gold.dim_content dc ON s.content_id = dc.content_id
JOIN gold.dim_date ddt ON s.start_ts::DATE = ddt.full_date
LEFT JOIN silver.content_genres cg ON s.content_id = cg.content_id
LEFT JOIN gold.dim_genres dg ON cg.genre_id = dg.genre_id
GROUP BY dc.content_key, dg.genre_key, ddt.date_key
ON CONFLICT DO NOTHING;

-- -----------------------------------------------------------------------------
-- 12.3. Poblar fact_ad_performance desde silver.ad_impressions
-- Agregación por anuncio, campaña, contenido y fecha
-- -----------------------------------------------------------------------------
INSERT INTO gold.fact_ad_performance (
    ad_id, campaign_id, content_key, date_key,
    total_impressions, total_clicks, click_through_rate,
    total_ad_time_seconds, unique_users
)
SELECT 
    ai.ad_id,
    a.campaign_id,
    dc.content_key,
    ddt.date_key,
    COUNT(ai.ad_impression_id) AS total_impressions,
    SUM(CASE WHEN ai.clicked = TRUE THEN 1 ELSE 0 END) AS total_clicks,
    CASE 
        WHEN COUNT(ai.ad_impression_id) > 0 
        THEN (SUM(CASE WHEN ai.clicked = TRUE THEN 1 ELSE 0 END)::NUMERIC / COUNT(ai.ad_impression_id)::NUMERIC) * 100
        ELSE 0
    END AS click_through_rate,
    SUM(a.duration_seconds) AS total_ad_time_seconds,
    COUNT(DISTINCT ai.user_id) AS unique_users
FROM silver.ad_impressions ai
JOIN silver.ads a ON ai.ad_id = a.ad_id
LEFT JOIN gold.dim_content dc ON ai.content_id = dc.content_id
JOIN gold.dim_date ddt ON ai.impression_ts::DATE = ddt.full_date
GROUP BY ai.ad_id, a.campaign_id, dc.content_key, ddt.date_key
ON CONFLICT DO NOTHING;

-- =============================================================================
-- PASO 13: ÍNDICES EN TABLAS DE HECHOS PARA OPTIMIZACIÓN
-- =============================================================================

-- Índices en fact_user_activity
CREATE INDEX IF NOT EXISTS idx_fact_user_activity_user ON gold.fact_user_activity(user_key);
CREATE INDEX IF NOT EXISTS idx_fact_user_activity_content ON gold.fact_user_activity(content_key);
CREATE INDEX IF NOT EXISTS idx_fact_user_activity_date ON gold.fact_user_activity(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_user_activity_device ON gold.fact_user_activity(device_key);

-- Índices en fact_content_popularity
CREATE INDEX IF NOT EXISTS idx_fact_content_pop_content ON gold.fact_content_popularity(content_key);
CREATE INDEX IF NOT EXISTS idx_fact_content_pop_genre ON gold.fact_content_popularity(genre_key);
CREATE INDEX IF NOT EXISTS idx_fact_content_pop_date ON gold.fact_content_popularity(date_key);

-- Índices en fact_ad_performance
CREATE INDEX IF NOT EXISTS idx_fact_ad_perf_ad ON gold.fact_ad_performance(ad_id);
CREATE INDEX IF NOT EXISTS idx_fact_ad_perf_content ON gold.fact_ad_performance(content_key);
CREATE INDEX IF NOT EXISTS idx_fact_ad_perf_date ON gold.fact_ad_performance(date_key);

-- =============================================================================
-- VERIFICACIÓN DE DATOS CARGADOS EN GOLD
-- =============================================================================

-- Verificar conteo de registros en dimensiones y hechos
SELECT 'dim_users' AS tabla, COUNT(*) AS registros FROM gold.dim_users
UNION ALL
SELECT 'dim_content', COUNT(*) FROM gold.dim_content
UNION ALL
SELECT 'dim_devices', COUNT(*) FROM gold.dim_devices
UNION ALL
SELECT 'dim_genres', COUNT(*) FROM gold.dim_genres
UNION ALL
SELECT 'dim_date', COUNT(*) FROM gold.dim_date
UNION ALL
SELECT 'dim_time_of_day', COUNT(*) FROM gold.dim_time_of_day
UNION ALL
SELECT 'fact_user_activity', COUNT(*) FROM gold.fact_user_activity
UNION ALL
SELECT 'fact_content_popularity', COUNT(*) FROM gold.fact_content_popularity
UNION ALL
SELECT 'fact_ad_performance', COUNT(*) FROM gold.fact_ad_performance;

