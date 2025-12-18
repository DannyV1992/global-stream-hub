-- =============================================================================
-- PARTE 3: TRANSFORMACIONES Y CARGA DE DATOS BRONZE -> SILVER
-- =============================================================================

-- =============================================================================
-- PASO 6: FUNCIONES AUXILIARES PARA TRANSFORMACIÓN
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 6.1. Función: parse_duration_to_minutes
-- Convierte diferentes formatos de duración a minutos (NUMERIC)
-- Ejemplos: "120 min", "2:00:00", "3:45", "45:30"
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION silver.parse_duration_to_minutes(duration_str VARCHAR)
RETURNS NUMERIC AS $$
DECLARE
    v_result NUMERIC := 0;
    v_parts TEXT[];
    v_hours NUMERIC := 0;
    v_minutes NUMERIC := 0;
    v_seconds NUMERIC := 0;
BEGIN
    -- Si es NULL o vacío, retornar 0
    IF duration_str IS NULL OR TRIM(duration_str) = '' THEN
        RETURN 0;
    END IF;

    -- Eliminar espacios y convertir a minúsculas
    duration_str := LOWER(TRIM(duration_str));

    -- Caso 1: Formato "120 min" o "120min"
    IF duration_str LIKE '%min%' THEN
        v_result := NULLIF(REGEXP_REPLACE(duration_str, '[^0-9.]', '', 'g'), '')::NUMERIC;
        RETURN v_result;
    END IF;

    -- Caso 2: Formato de tiempo "HH:MM:SS" o "MM:SS" o "H:MM:SS"
    IF duration_str LIKE '%:%' THEN
        v_parts := STRING_TO_ARRAY(duration_str, ':');

        -- Formato HH:MM:SS
        IF ARRAY_LENGTH(v_parts, 1) = 3 THEN
            v_hours := COALESCE(v_parts[1]::NUMERIC, 0);
            v_minutes := COALESCE(v_parts[2]::NUMERIC, 0);
            v_seconds := COALESCE(v_parts[3]::NUMERIC, 0);
            v_result := (v_hours * 60) + v_minutes + (v_seconds / 60);
            RETURN v_result;
        END IF;

        -- Formato MM:SS
        IF ARRAY_LENGTH(v_parts, 1) = 2 THEN
            v_minutes := COALESCE(v_parts[1]::NUMERIC, 0);
            v_seconds := COALESCE(v_parts[2]::NUMERIC, 0);
            v_result := v_minutes + (v_seconds / 60);
            RETURN v_result;
        END IF;
    END IF;

    -- Caso 3: Solo número (asumimos minutos)
    IF duration_str ~ '^[0-9.]+$' THEN
        v_result := duration_str::NUMERIC;
        RETURN v_result;
    END IF;

    -- Si no coincide con ningún patrón, retornar 0
    RETURN 0;

EXCEPTION
    WHEN OTHERS THEN
        -- En caso de error en el parsing, retornar 0
        RETURN 0;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION silver.parse_duration_to_minutes IS 'Convierte diferentes formatos de duración a minutos decimales.';

-- -----------------------------------------------------------------------------
-- 6.2. Función: parse_percentage
-- Convierte porcentajes en diferentes formatos a decimal
-- Ejemplos: "100%", "37.5%", "85", "100"
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION silver.parse_percentage(pct_str VARCHAR)
RETURNS NUMERIC AS $$
DECLARE
    v_result NUMERIC := 0;
    v_clean VARCHAR;
BEGIN
    -- Si es NULL o vacío, retornar 0
    IF pct_str IS NULL OR TRIM(pct_str) = '' THEN
        RETURN 0;
    END IF;

    -- Eliminar el símbolo % y espacios
    v_clean := TRIM(REPLACE(pct_str, '%', ''));

    -- Convertir a numeric
    v_result := v_clean::NUMERIC;

    -- Si el valor es mayor a 100, asumimos que está en escala 0-100
    -- Si es menor o igual a 100, lo dejamos así
    -- Si es menor a 1, asumimos que ya está en escala decimal (ej: 0.85)
    IF v_result <= 1 AND v_result > 0 THEN
        v_result := v_result * 100;
    END IF;

    RETURN v_result;

EXCEPTION
    WHEN OTHERS THEN
        RETURN 0;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION silver.parse_percentage IS 'Convierte string de porcentaje a valor decimal normalizado.';

-- -----------------------------------------------------------------------------
-- 6.3. Función: parse_boolean
-- Convierte diferentes representaciones de boolean a BOOLEAN
-- Ejemplos: "true", "false", "1", "0", "yes", "no"
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION silver.parse_boolean(bool_str VARCHAR)
RETURNS BOOLEAN AS $$
BEGIN
    IF bool_str IS NULL OR TRIM(bool_str) = '' THEN
        RETURN FALSE;
    END IF;

    bool_str := LOWER(TRIM(bool_str));

    IF bool_str IN ('true', '1', 't', 'yes', 'y') THEN
        RETURN TRUE;
    ELSE
        RETURN FALSE;
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        RETURN FALSE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION silver.parse_boolean IS 'Convierte representaciones string de boolean a tipo BOOLEAN.';

-- -----------------------------------------------------------------------------
-- 6.4. Función: clean_currency_amount
-- Limpia montos con símbolos de moneda y los convierte a NUMERIC
-- Ejemplos: "$14.99", "14.99", "12.99"
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION silver.clean_currency_amount(amount_str VARCHAR)
RETURNS NUMERIC AS $$
DECLARE
    v_clean VARCHAR;
    v_result NUMERIC := 0;
BEGIN
    IF amount_str IS NULL OR TRIM(amount_str) = '' THEN
        RETURN 0;
    END IF;

    -- Eliminar símbolos de moneda y espacios
    v_clean := REGEXP_REPLACE(amount_str, '[^0-9.]', '', 'g');

    IF v_clean = '' THEN
        RETURN 0;
    END IF;

    v_result := v_clean::NUMERIC;
    RETURN v_result;

EXCEPTION
    WHEN OTHERS THEN
        RETURN 0;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION silver.clean_currency_amount IS 'Limpia y convierte montos monetarios a NUMERIC.';

-- =============================================================================
-- PASO 7: CARGA DE CATÁLOGOS BASE EN SILVER
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 7.1. Cargar países desde Bronze
-- -----------------------------------------------------------------------------
INSERT INTO silver.countries (country_name, iso_code, region)
SELECT DISTINCT 
    TRIM(pais) AS country_name,
    NULL AS iso_code,  -- Se puede enriquecer posteriormente
    NULL AS region     -- Se puede enriquecer posteriormente
FROM bronze.user_registrations
WHERE TRIM(pais) IS NOT NULL AND TRIM(pais) != ''
ON CONFLICT DO NOTHING;

-- Actualizar códigos ISO manualmente (opcional, para mejorar calidad)
UPDATE silver.countries SET iso_code = 'CR', region = 'Central America' WHERE country_name = 'Costa Rica';
UPDATE silver.countries SET iso_code = 'MX', region = 'North America' WHERE country_name = 'México';
UPDATE silver.countries SET iso_code = 'ES', region = 'Europe' WHERE country_name = 'España';
UPDATE silver.countries SET iso_code = 'AR', region = 'South America' WHERE country_name = 'Argentina';
UPDATE silver.countries SET iso_code = 'CO', region = 'South America' WHERE country_name = 'Colombia';
UPDATE silver.countries SET iso_code = 'CL', region = 'South America' WHERE country_name = 'Chile';
UPDATE silver.countries SET iso_code = 'PE', region = 'South America' WHERE country_name = 'Perú';
UPDATE silver.countries SET iso_code = 'EC', region = 'South America' WHERE country_name = 'Ecuador';

-- -----------------------------------------------------------------------------
-- 7.2. Cargar planes de suscripción
-- -----------------------------------------------------------------------------
INSERT INTO silver.subscription_plans (plan_name, monthly_price, description) VALUES
('Premium', 14.99, 'Acceso sin publicidad y contenido exclusivo'),
('Con Publicidad', 4.99, 'Acceso al contenido con anuncios intercalados'),
('Gratis', 0.00, 'Plan gratuito con limitaciones y publicidad')
ON CONFLICT DO NOTHING;

-- -----------------------------------------------------------------------------
-- 7.3. Cargar usuarios desde Bronze
-- -----------------------------------------------------------------------------
INSERT INTO silver.users (source_user_id, full_name, email, country_id, registration_ts, current_plan_id)
SELECT 
    b.user_id AS source_user_id,
    TRIM(b.nombre) AS full_name,
    LOWER(TRIM(b.email)) AS email,
    c.country_id,
    b.fecha_registro::TIMESTAMP WITH TIME ZONE AS registration_ts,
    p.plan_id AS current_plan_id
FROM bronze.user_registrations b
LEFT JOIN silver.countries c ON TRIM(b.pais) = c.country_name
LEFT JOIN silver.subscription_plans p ON TRIM(b.tipo_suscripcion) = p.plan_name
WHERE b.user_id IS NOT NULL
ON CONFLICT (email) DO NOTHING;

-- -----------------------------------------------------------------------------
-- 7.4. Cargar géneros desde Bronze (normalizados)
-- Primero extraemos géneros únicos del campo "genero" que puede tener múltiples
-- valores separados por comas
-- -----------------------------------------------------------------------------

-- Crear una tabla temporal con géneros separados
CREATE TEMP TABLE temp_genres AS
SELECT DISTINCT 
    TRIM(genre_split) AS genre_name
FROM bronze.raw_catalog_data,
LATERAL UNNEST(STRING_TO_ARRAY(genero, ',')) AS genre_split
WHERE genero IS NOT NULL;

-- Insertar géneros únicos
INSERT INTO silver.genres (genre_name)
SELECT DISTINCT genre_name
FROM temp_genres
WHERE genre_name != ''
ON CONFLICT DO NOTHING;

-- -----------------------------------------------------------------------------
-- 7.5. Cargar artistas/directores desde Bronze
-- Extraer y normalizar nombres de director_artista
-- -----------------------------------------------------------------------------

-- Crear tabla temporal con artistas separados
CREATE TEMP TABLE temp_artists AS
SELECT DISTINCT 
    TRIM(artist_split) AS artist_name
FROM bronze.raw_catalog_data,
LATERAL UNNEST(STRING_TO_ARRAY(director_artista, ',')) AS artist_split
WHERE director_artista IS NOT NULL;

-- Insertar artistas únicos
INSERT INTO silver.artists_directors (name, role_type)
SELECT DISTINCT 
    artist_name,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM bronze.raw_catalog_data 
            WHERE tipo = 'video' AND director_artista LIKE '%' || artist_name || '%'
        ) THEN 'director'
        ELSE 'artista'
    END AS role_type
FROM temp_artists
WHERE artist_name != ''
ON CONFLICT DO NOTHING;

-- -----------------------------------------------------------------------------
-- 7.6. Cargar contenido desde Bronze
-- -----------------------------------------------------------------------------
INSERT INTO silver.content (source_content_id, title, content_type, release_year, duration_minutes, description)
SELECT 
    b.content_id AS source_content_id,
    TRIM(b.titulo) AS title,
    LOWER(TRIM(b.tipo)) AS content_type,
    NULLIF(TRIM(b.anio), '')::INT AS release_year,
    silver.parse_duration_to_minutes(b.duracion) AS duration_minutes,
    TRIM(b.descripcion) AS description
FROM bronze.raw_catalog_data b
WHERE b.content_id IS NOT NULL
ON CONFLICT DO NOTHING;

-- -----------------------------------------------------------------------------
-- 7.7. Relacionar contenido con géneros (content_genres)
-- -----------------------------------------------------------------------------
INSERT INTO silver.content_genres (content_id, genre_id)
SELECT DISTINCT
    c.content_id,
    g.genre_id
FROM bronze.raw_catalog_data b
CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(b.genero, ',')) AS genre_split
JOIN silver.content c ON c.source_content_id = b.content_id
JOIN silver.genres g ON TRIM(genre_split) = g.genre_name
WHERE b.genero IS NOT NULL
ON CONFLICT DO NOTHING;

-- -----------------------------------------------------------------------------
-- 7.8. Relacionar contenido con artistas (content_artists)
-- -----------------------------------------------------------------------------
INSERT INTO silver.content_artists (content_id, artist_id, role_description)
SELECT DISTINCT
    c.content_id,
    a.artist_id,
    CASE 
        WHEN b.tipo = 'video' THEN 'Director'
        ELSE 'Artista'
    END AS role_description
FROM bronze.raw_catalog_data b
CROSS JOIN LATERAL UNNEST(STRING_TO_ARRAY(b.director_artista, ',')) AS artist_split
JOIN silver.content c ON c.source_content_id = b.content_id
JOIN silver.artists_directors a ON TRIM(artist_split) = a.name
WHERE b.director_artista IS NOT NULL
ON CONFLICT DO NOTHING;

-- -----------------------------------------------------------------------------
-- 7.9. Cargar dispositivos desde Bronze (normalizados)
-- -----------------------------------------------------------------------------
INSERT INTO silver.devices (device_type, operating_system)
SELECT DISTINCT 
    TRIM(dispositivo) AS device_type,
    NULL AS operating_system  -- Se puede enriquecer desde metadata_raw si está disponible
FROM bronze.raw_streaming_logs
WHERE dispositivo IS NOT NULL
ON CONFLICT DO NOTHING;

-- -----------------------------------------------------------------------------
-- 7.10. Cargar sesiones de streaming desde Bronze
-- -----------------------------------------------------------------------------
INSERT INTO silver.streaming_sessions (user_id, content_id, device_id, start_ts, end_ts, duration_seconds, watched_pct)
SELECT 
    u.user_id,
    c.content_id,
    d.device_id,
    b.timestamp_inicio::TIMESTAMP WITH TIME ZONE AS start_ts,
    b.timestamp_fin::TIMESTAMP WITH TIME ZONE AS end_ts,
    EXTRACT(EPOCH FROM (b.timestamp_fin::TIMESTAMP - b.timestamp_inicio::TIMESTAMP)) AS duration_seconds,
    silver.parse_percentage(b.tiempo_visto_pct) AS watched_pct
FROM bronze.raw_streaming_logs b
JOIN silver.users u ON u.source_user_id = b.user_id
JOIN silver.content c ON c.source_content_id = b.content_id
LEFT JOIN silver.devices d ON TRIM(b.dispositivo) = d.device_type
WHERE b.user_id IS NOT NULL 
  AND b.content_id IS NOT NULL
ON CONFLICT DO NOTHING;

-- -----------------------------------------------------------------------------
-- 7.11. Cargar suscripciones desde Bronze
-- -----------------------------------------------------------------------------
INSERT INTO silver.subscriptions (source_subscription_id, user_id, plan_id, start_date, end_date, payment_status, payment_amount, currency)
SELECT 
    b.subscription_id AS source_subscription_id,
    u.user_id,
    p.plan_id,
    b.start_date::DATE AS start_date,
    b.end_date::DATE AS end_date,
    LOWER(TRIM(b.payment_status)) AS payment_status,
    silver.clean_currency_amount(b.payment_amount) AS payment_amount,
    UPPER(TRIM(b.currency)) AS currency
FROM bronze.raw_subscription_data b
JOIN silver.users u ON u.source_user_id = b.user_id
JOIN silver.subscription_plans p ON TRIM(b.plan_type) = p.plan_name
WHERE b.subscription_id IS NOT NULL
ON CONFLICT DO NOTHING;

-- -----------------------------------------------------------------------------
-- 7.12. Crear campañas publicitarias (datos sintéticos basados en advertisers)
-- -----------------------------------------------------------------------------
INSERT INTO silver.ad_campaigns (campaign_name, advertiser, start_date, end_date, budget_amount)
SELECT DISTINCT
    'Campaña ' || (b.metadata_raw->>'advertiser') AS campaign_name,
    (b.metadata_raw->>'advertiser') AS advertiser,
    DATE('2024-01-01') AS start_date,
    DATE('2024-12-31') AS end_date,
    50000.00 AS budget_amount
FROM bronze.raw_ad_impressions b
WHERE b.metadata_raw->>'advertiser' IS NOT NULL
ON CONFLICT DO NOTHING;

-- -----------------------------------------------------------------------------
-- 7.13. Cargar anuncios desde Bronze
-- -----------------------------------------------------------------------------
INSERT INTO silver.ads (source_ad_id, campaign_id, title, duration_seconds)
SELECT DISTINCT
    b.ad_id AS source_ad_id,
    ac.campaign_id,
    'Anuncio ' || b.ad_id AS title,
    (b.metadata_raw->>'ad_duration_sec')::NUMERIC AS duration_seconds
FROM bronze.raw_ad_impressions b
LEFT JOIN silver.ad_campaigns ac ON ac.advertiser = (b.metadata_raw->>'advertiser')
WHERE b.ad_id IS NOT NULL
ON CONFLICT DO NOTHING;

-- -----------------------------------------------------------------------------
-- 7.14. Cargar impresiones de anuncios desde Bronze
-- Relacionar con sesiones de streaming cuando sea posible
-- -----------------------------------------------------------------------------
INSERT INTO silver.ad_impressions (ad_id, user_id, content_id, session_id, impression_ts, ad_placement, clicked)
SELECT 
    a.ad_id,
    u.user_id,
    c.content_id,
    s.session_id,
    b.timestamp::TIMESTAMP WITH TIME ZONE AS impression_ts,
    TRIM(b.ad_placement) AS ad_placement,
    silver.parse_boolean(b.clicked) AS clicked
FROM bronze.raw_ad_impressions b
LEFT JOIN silver.ads a ON a.source_ad_id = b.ad_id
LEFT JOIN silver.users u ON u.source_user_id = b.user_id
LEFT JOIN silver.content c ON c.source_content_id = b.content_id
LEFT JOIN silver.streaming_sessions s ON s.user_id = u.user_id 
    AND s.content_id = c.content_id
    AND s.start_ts <= b.timestamp::TIMESTAMP WITH TIME ZONE
    AND (s.end_ts IS NULL OR s.end_ts >= b.timestamp::TIMESTAMP WITH TIME ZONE)
WHERE b.ad_id IS NOT NULL
ON CONFLICT DO NOTHING;

-- =============================================================================
-- PASO 8: CREACIÓN DE ÍNDICES EN CAPA SILVER
-- =============================================================================

-- Índices para optimizar consultas frecuentes

-- Índices en users
CREATE INDEX IF NOT EXISTS idx_users_country ON silver.users(country_id);
CREATE INDEX IF NOT EXISTS idx_users_plan ON silver.users(current_plan_id);
CREATE INDEX IF NOT EXISTS idx_users_email ON silver.users(email);

-- Índices en subscriptions
CREATE INDEX IF NOT EXISTS idx_subscriptions_user ON silver.subscriptions(user_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_plan ON silver.subscriptions(plan_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_dates ON silver.subscriptions(start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_subscriptions_status ON silver.subscriptions(payment_status);

-- Índices en content
CREATE INDEX IF NOT EXISTS idx_content_type ON silver.content(content_type);
CREATE INDEX IF NOT EXISTS idx_content_year ON silver.content(release_year);

-- Índices en content_genres
CREATE INDEX IF NOT EXISTS idx_content_genres_content ON silver.content_genres(content_id);
CREATE INDEX IF NOT EXISTS idx_content_genres_genre ON silver.content_genres(genre_id);

-- Índices en content_artists
CREATE INDEX IF NOT EXISTS idx_content_artists_content ON silver.content_artists(content_id);
CREATE INDEX IF NOT EXISTS idx_content_artists_artist ON silver.content_artists(artist_id);

-- Índices en streaming_sessions
CREATE INDEX IF NOT EXISTS idx_sessions_user ON silver.streaming_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_content ON silver.streaming_sessions(content_id);
CREATE INDEX IF NOT EXISTS idx_sessions_device ON silver.streaming_sessions(device_id);
CREATE INDEX IF NOT EXISTS idx_sessions_start_ts ON silver.streaming_sessions(start_ts);

-- Índices en ad_impressions
CREATE INDEX IF NOT EXISTS idx_ad_impressions_ad ON silver.ad_impressions(ad_id);
CREATE INDEX IF NOT EXISTS idx_ad_impressions_user ON silver.ad_impressions(user_id);
CREATE INDEX IF NOT EXISTS idx_ad_impressions_content ON silver.ad_impressions(content_id);
CREATE INDEX IF NOT EXISTS idx_ad_impressions_ts ON silver.ad_impressions(impression_ts);
CREATE INDEX IF NOT EXISTS idx_ad_impressions_clicked ON silver.ad_impressions(clicked);

-- =============================================================================
-- VERIFICACIÓN DE DATOS CARGADOS
-- =============================================================================

-- Verificar conteo de registros en tablas principales
SELECT 'users' AS tabla, COUNT(*) AS registros FROM silver.users
UNION ALL
SELECT 'countries', COUNT(*) FROM silver.countries
UNION ALL
SELECT 'subscription_plans', COUNT(*) FROM silver.subscription_plans
UNION ALL
SELECT 'subscriptions', COUNT(*) FROM silver.subscriptions
UNION ALL
SELECT 'content', COUNT(*) FROM silver.content
UNION ALL
SELECT 'genres', COUNT(*) FROM silver.genres
UNION ALL
SELECT 'artists_directors', COUNT(*) FROM silver.artists_directors
UNION ALL
SELECT 'content_genres', COUNT(*) FROM silver.content_genres
UNION ALL
SELECT 'content_artists', COUNT(*) FROM silver.content_artists
UNION ALL
SELECT 'devices', COUNT(*) FROM silver.devices
UNION ALL
SELECT 'streaming_sessions', COUNT(*) FROM silver.streaming_sessions
UNION ALL
SELECT 'ad_campaigns', COUNT(*) FROM silver.ad_campaigns
UNION ALL
SELECT 'ads', COUNT(*) FROM silver.ads
UNION ALL
SELECT 'ad_impressions', COUNT(*) FROM silver.ad_impressions;
