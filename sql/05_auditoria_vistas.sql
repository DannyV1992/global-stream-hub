-- =============================================================================
-- PARTE 5: AUDITORÍA, TRIGGERS, VISTAS Y VISTAS MATERIALIZADAS (VERSIÓN FINAL)
-- =============================================================================

-- =============================================================================
-- PASO 14: SISTEMA DE AUDITORÍA
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 14.1. Tabla: audit.operation_log
-- Log centralizado de todas las operaciones importantes en la BD
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.operation_log (
    log_id             BIGSERIAL PRIMARY KEY,
    schema_name        VARCHAR(100),
    table_name         VARCHAR(100),
    operation_type     VARCHAR(20),           -- INSERT, UPDATE, DELETE
    user_db            VARCHAR(100),
    timestamp          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    affected_rows      INT,
    description        TEXT
);

COMMENT ON TABLE audit.operation_log IS 'Log centralizado de operaciones en la base de datos.';

-- -----------------------------------------------------------------------------
-- 14.2. Tabla: audit.history_users
-- Historial de cambios en usuarios
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.history_users (
    history_id         BIGSERIAL PRIMARY KEY,
    user_id            INT NOT NULL,
    operation_type     VARCHAR(20),           -- INSERT, UPDATE, DELETE
    old_data           JSONB,
    new_data           JSONB,
    changed_by         VARCHAR(100),
    changed_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE audit.history_users IS 'Historial de cambios en la tabla silver.users.';

-- -----------------------------------------------------------------------------
-- 14.3. Tabla: audit.history_subscriptions
-- Historial de cambios en suscripciones
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.history_subscriptions (
    history_id         BIGSERIAL PRIMARY KEY,
    subscription_id    INT NOT NULL,
    operation_type     VARCHAR(20),
    old_data           JSONB,
    new_data           JSONB,
    changed_by         VARCHAR(100),
    changed_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE audit.history_subscriptions IS 'Historial de cambios en la tabla silver.subscriptions.';

-- -----------------------------------------------------------------------------
-- 14.4. Tabla: audit.history_content
-- Historial de cambios en contenido
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.history_content (
    history_id         BIGSERIAL PRIMARY KEY,
    content_id         INT NOT NULL,
    operation_type     VARCHAR(20),
    old_data           JSONB,
    new_data           JSONB,
    changed_by         VARCHAR(100),
    changed_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE audit.history_content IS 'Historial de cambios en la tabla silver.content.';

-- =============================================================================
-- PASO 15: FUNCIONES Y TRIGGERS DE AUDITORÍA
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 15.1. Función genérica para auditar cambios en usuarios
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION audit.audit_users_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO audit.history_users (user_id, operation_type, new_data, changed_by)
        VALUES (NEW.user_id, 'INSERT', row_to_json(NEW)::JSONB, current_user);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit.history_users (user_id, operation_type, old_data, new_data, changed_by)
        VALUES (NEW.user_id, 'UPDATE', row_to_json(OLD)::JSONB, row_to_json(NEW)::JSONB, current_user);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO audit.history_users (user_id, operation_type, old_data, changed_by)
        VALUES (OLD.user_id, 'DELETE', row_to_json(OLD)::JSONB, current_user);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION audit.audit_users_changes IS 'Función de trigger para auditar cambios en silver.users.';

-- Eliminar trigger si existe y crear nuevo
DROP TRIGGER IF EXISTS trg_audit_users ON silver.users;

CREATE TRIGGER trg_audit_users
AFTER INSERT OR UPDATE OR DELETE ON silver.users
FOR EACH ROW EXECUTE FUNCTION audit.audit_users_changes();

-- -----------------------------------------------------------------------------
-- 15.2. Función genérica para auditar cambios en suscripciones
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION audit.audit_subscriptions_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO audit.history_subscriptions (subscription_id, operation_type, new_data, changed_by)
        VALUES (NEW.subscription_id, 'INSERT', row_to_json(NEW)::JSONB, current_user);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit.history_subscriptions (subscription_id, operation_type, old_data, new_data, changed_by)
        VALUES (NEW.subscription_id, 'UPDATE', row_to_json(OLD)::JSONB, row_to_json(NEW)::JSONB, current_user);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO audit.history_subscriptions (subscription_id, operation_type, old_data, changed_by)
        VALUES (OLD.subscription_id, 'DELETE', row_to_json(OLD)::JSONB, current_user);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION audit.audit_subscriptions_changes IS 'Función de trigger para auditar cambios en silver.subscriptions.';

-- Eliminar trigger si existe y crear nuevo
DROP TRIGGER IF EXISTS trg_audit_subscriptions ON silver.subscriptions;

CREATE TRIGGER trg_audit_subscriptions
AFTER INSERT OR UPDATE OR DELETE ON silver.subscriptions
FOR EACH ROW EXECUTE FUNCTION audit.audit_subscriptions_changes();

-- -----------------------------------------------------------------------------
-- 15.3. Función genérica para auditar cambios en contenido
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION audit.audit_content_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO audit.history_content (content_id, operation_type, new_data, changed_by)
        VALUES (NEW.content_id, 'INSERT', row_to_json(NEW)::JSONB, current_user);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit.history_content (content_id, operation_type, old_data, new_data, changed_by)
        VALUES (NEW.content_id, 'UPDATE', row_to_json(OLD)::JSONB, row_to_json(NEW)::JSONB, current_user);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO audit.history_content (content_id, operation_type, old_data, changed_by)
        VALUES (OLD.content_id, 'DELETE', row_to_json(OLD)::JSONB, current_user);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION audit.audit_content_changes IS 'Función de trigger para auditar cambios en silver.content.';

-- Eliminar trigger si existe y crear nuevo
DROP TRIGGER IF EXISTS trg_audit_content ON silver.content;

CREATE TRIGGER trg_audit_content
AFTER INSERT OR UPDATE OR DELETE ON silver.content
FOR EACH ROW EXECUTE FUNCTION audit.audit_content_changes();

-- -----------------------------------------------------------------------------
-- 15.4. Función para actualizar timestamp updated_at automáticamente
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION silver.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION silver.update_updated_at_column IS 'Actualiza automáticamente el campo updated_at en tablas Silver.';

-- Crear triggers para actualizar updated_at en tablas relevantes
-- Eliminar triggers existentes primero

DROP TRIGGER IF EXISTS trg_users_updated_at ON silver.users;
CREATE TRIGGER trg_users_updated_at
BEFORE UPDATE ON silver.users
FOR EACH ROW EXECUTE FUNCTION silver.update_updated_at_column();

DROP TRIGGER IF EXISTS trg_subscriptions_updated_at ON silver.subscriptions;
CREATE TRIGGER trg_subscriptions_updated_at
BEFORE UPDATE ON silver.subscriptions
FOR EACH ROW EXECUTE FUNCTION silver.update_updated_at_column();

DROP TRIGGER IF EXISTS trg_content_updated_at ON silver.content;
CREATE TRIGGER trg_content_updated_at
BEFORE UPDATE ON silver.content
FOR EACH ROW EXECUTE FUNCTION silver.update_updated_at_column();

DROP TRIGGER IF EXISTS trg_subscription_plans_updated_at ON silver.subscription_plans;
CREATE TRIGGER trg_subscription_plans_updated_at
BEFORE UPDATE ON silver.subscription_plans
FOR EACH ROW EXECUTE FUNCTION silver.update_updated_at_column();

DROP TRIGGER IF EXISTS trg_playlists_updated_at ON silver.playlists;
CREATE TRIGGER trg_playlists_updated_at
BEFORE UPDATE ON silver.playlists
FOR EACH ROW EXECUTE FUNCTION silver.update_updated_at_column();

-- =============================================================================
-- PASO 16: VISTAS DE NEGOCIO (VIEWS)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 16.1. Vista: vw_user_profile_details
-- Información completa del perfil de usuario con última actividad
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_user_profile_details AS
SELECT 
    u.user_id,
    u.full_name,
    u.email,
    c.country_name,
    c.iso_code AS country_code,
    c.region,
    p.plan_name AS subscription_plan,
    p.monthly_price AS plan_price,
    s.payment_status AS subscription_status,
    s.start_date AS subscription_start_date,
    s.end_date AS subscription_end_date,
    u.registration_ts AS registration_date,
    u.active AS is_active,
    MAX(ss.start_ts) AS last_activity_date,
    COUNT(DISTINCT ss.session_id) AS total_sessions,
    SUM(ss.duration_seconds) / 60.0 AS total_watch_time_minutes
FROM silver.users u
LEFT JOIN silver.countries c ON u.country_id = c.country_id
LEFT JOIN silver.subscription_plans p ON u.current_plan_id = p.plan_id
LEFT JOIN silver.subscriptions s ON s.user_id = u.user_id 
    AND s.payment_status = 'active'
LEFT JOIN silver.streaming_sessions ss ON ss.user_id = u.user_id
GROUP BY 
    u.user_id, u.full_name, u.email, c.country_name, c.iso_code, c.region,
    p.plan_name, p.monthly_price, s.payment_status, s.start_date, s.end_date,
    u.registration_ts, u.active;

COMMENT ON VIEW gold.vw_user_profile_details IS 'Perfil completo del usuario con información de suscripción y actividad.';

-- -----------------------------------------------------------------------------
-- 16.2. Vista: vw_content_catalog_summary
-- Resumen del catálogo de contenido con popularidad
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_content_catalog_summary AS
SELECT 
    c.content_id,
    c.title,
    c.content_type,
    c.release_year,
    c.duration_minutes,
    STRING_AGG(DISTINCT g.genre_name, ', ' ORDER BY g.genre_name) AS genres,
    STRING_AGG(DISTINCT a.name, ', ' ORDER BY a.name) AS artists_directors,
    COUNT(DISTINCT ss.session_id) AS total_views,
    COUNT(DISTINCT ss.user_id) AS unique_viewers,
    ROUND(AVG(ss.watched_pct), 2) AS avg_completion_rate,
    SUM(ss.duration_seconds) / 60.0 AS total_watch_time_minutes,
    c.created_at
FROM silver.content c
LEFT JOIN silver.content_genres cg ON c.content_id = cg.content_id
LEFT JOIN silver.genres g ON cg.genre_id = g.genre_id
LEFT JOIN silver.content_artists ca ON c.content_id = ca.content_id
LEFT JOIN silver.artists_directors a ON ca.artist_id = a.artist_id
LEFT JOIN silver.streaming_sessions ss ON c.content_id = ss.content_id
GROUP BY c.content_id, c.title, c.content_type, c.release_year, c.duration_minutes, c.created_at
ORDER BY total_views DESC NULLS LAST;

COMMENT ON VIEW gold.vw_content_catalog_summary IS 'Resumen del catálogo con métricas de popularidad.';

-- -----------------------------------------------------------------------------
-- 16.3. Vista: vw_user_playback_history
-- Historial de reproducciones de usuarios
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_user_playback_history AS
SELECT 
    u.user_id,
    u.full_name AS user_name,
    u.email AS user_email,
    c.content_id,
    c.title AS content_title,
    c.content_type,
    c.duration_minutes AS content_duration_minutes,
    ss.start_ts AS playback_start,
    ss.end_ts AS playback_end,
    ROUND(ss.duration_seconds / 60.0, 2) AS session_duration_minutes,
    ss.watched_pct AS completion_percentage,
    d.device_type,
    STRING_AGG(DISTINCT g.genre_name, ', ') AS genres
FROM silver.streaming_sessions ss
JOIN silver.users u ON ss.user_id = u.user_id
JOIN silver.content c ON ss.content_id = c.content_id
LEFT JOIN silver.devices d ON ss.device_id = d.device_id
LEFT JOIN silver.content_genres cg ON c.content_id = cg.content_id
LEFT JOIN silver.genres g ON cg.genre_id = g.genre_id
GROUP BY 
    u.user_id, u.full_name, u.email, c.content_id, c.title, c.content_type,
    c.duration_minutes, ss.session_id, ss.start_ts, ss.end_ts, ss.duration_seconds,
    ss.watched_pct, d.device_type
ORDER BY ss.start_ts DESC;

COMMENT ON VIEW gold.vw_user_playback_history IS 'Historial detallado de reproducciones por usuario.';

-- -----------------------------------------------------------------------------
-- 16.4. Vista: vw_playlist_details
-- Detalles de playlists creadas por usuarios
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_playlist_details AS
SELECT 
    pl.playlist_id,
    pl.user_id,
    u.full_name AS user_name,
    pl.name AS playlist_name,
    pl.description AS playlist_description,
    pl.created_at AS playlist_created_at,
    COUNT(pi.playlist_item_id) AS total_items,
    STRING_AGG(c.title, ' | ' ORDER BY pi.position) AS content_titles,
    SUM(c.duration_minutes) AS total_duration_minutes
FROM silver.playlists pl
JOIN silver.users u ON pl.user_id = u.user_id
LEFT JOIN silver.playlist_items pi ON pl.playlist_id = pi.playlist_id
LEFT JOIN silver.content c ON pi.content_id = c.content_id
GROUP BY 
    pl.playlist_id, pl.user_id, u.full_name, pl.name, 
    pl.description, pl.created_at
ORDER BY pl.created_at DESC;

COMMENT ON VIEW gold.vw_playlist_details IS 'Detalles completos de playlists con su contenido.';

-- -----------------------------------------------------------------------------
-- 16.5. Vista: vw_ad_performance_summary
-- Resumen de rendimiento de anuncios por campaña
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW gold.vw_ad_performance_summary AS
SELECT 
    ac.campaign_id,
    ac.campaign_name,
    ac.advertiser,
    ac.start_date AS campaign_start_date,
    ac.end_date AS campaign_end_date,
    COUNT(DISTINCT a.ad_id) AS total_ads,
    COUNT(ai.ad_impression_id) AS total_impressions,
    SUM(CASE WHEN ai.clicked = TRUE THEN 1 ELSE 0 END) AS total_clicks,
    CASE 
        WHEN COUNT(ai.ad_impression_id) > 0 
        THEN ROUND((SUM(CASE WHEN ai.clicked = TRUE THEN 1 ELSE 0 END)::NUMERIC / 
                    COUNT(ai.ad_impression_id)::NUMERIC) * 100, 2)
        ELSE 0
    END AS click_through_rate_pct,
    COUNT(DISTINCT ai.user_id) AS unique_users_reached,
    SUM(a.duration_seconds) AS total_ad_time_seconds
FROM silver.ad_campaigns ac
LEFT JOIN silver.ads a ON ac.campaign_id = a.campaign_id
LEFT JOIN silver.ad_impressions ai ON a.ad_id = ai.ad_id
GROUP BY 
    ac.campaign_id, ac.campaign_name, ac.advertiser, 
    ac.start_date, ac.end_date
ORDER BY total_impressions DESC NULLS LAST;

COMMENT ON VIEW gold.vw_ad_performance_summary IS 'Resumen del rendimiento de campañas publicitarias con CTR.';

-- =============================================================================
-- PASO 17: VISTAS MATERIALIZADAS PARA ALTO RENDIMIENTO
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 17.1. Vista Materializada: mv_monthly_user_activity
-- Actividad mensual agregada por usuario
-- -----------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS gold.mv_monthly_user_activity CASCADE;

CREATE MATERIALIZED VIEW gold.mv_monthly_user_activity AS
SELECT 
    u.user_id,
    u.full_name,
    u.email,
    c.country_name,
    p.plan_name,
    DATE_TRUNC('month', ss.start_ts)::DATE AS activity_month,
    COUNT(DISTINCT ss.session_id) AS total_sessions,
    COUNT(DISTINCT ss.content_id) AS unique_content_watched,
    SUM(ss.duration_seconds) / 3600.0 AS total_hours_watched,
    ROUND(AVG(ss.watched_pct), 2) AS avg_completion_rate,
    SUM(CASE WHEN ss.watched_pct >= 90 THEN 1 ELSE 0 END) AS completed_content_count
FROM silver.streaming_sessions ss
JOIN silver.users u ON ss.user_id = u.user_id
LEFT JOIN silver.countries c ON u.country_id = c.country_id
LEFT JOIN silver.subscription_plans p ON u.current_plan_id = p.plan_id
GROUP BY 
    u.user_id, u.full_name, u.email, c.country_name, p.plan_name,
    DATE_TRUNC('month', ss.start_ts)::DATE;

COMMENT ON MATERIALIZED VIEW gold.mv_monthly_user_activity IS 'Actividad mensual agregada por usuario para análisis de tendencias.';

-- Crear índice único en la MV
CREATE UNIQUE INDEX idx_mv_monthly_user_activity 
ON gold.mv_monthly_user_activity (user_id, activity_month);

-- -----------------------------------------------------------------------------
-- 17.2. Vista Materializada: mv_content_popularity_by_genre
-- Popularidad de contenido por género
-- -----------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS gold.mv_content_popularity_by_genre CASCADE;

CREATE MATERIALIZED VIEW gold.mv_content_popularity_by_genre AS
SELECT 
    g.genre_id,
    g.genre_name,
    c.content_type,
    COUNT(DISTINCT c.content_id) AS total_content_items,
    COUNT(DISTINCT ss.session_id) AS total_views,
    COUNT(DISTINCT ss.user_id) AS unique_viewers,
    SUM(ss.duration_seconds) / 3600.0 AS total_hours_watched,
    ROUND(AVG(ss.watched_pct), 2) AS avg_completion_rate
FROM silver.genres g
JOIN silver.content_genres cg ON g.genre_id = cg.genre_id
JOIN silver.content c ON cg.content_id = c.content_id
LEFT JOIN silver.streaming_sessions ss ON c.content_id = ss.content_id
GROUP BY g.genre_id, g.genre_name, c.content_type
ORDER BY total_views DESC NULLS LAST;

COMMENT ON MATERIALIZED VIEW gold.mv_content_popularity_by_genre IS 'Popularidad de contenido agregada por género.';

-- Crear índice único en la MV
CREATE UNIQUE INDEX idx_mv_content_pop_genre 
ON gold.mv_content_popularity_by_genre (genre_id, content_type);

-- -----------------------------------------------------------------------------
-- 17.3. Vista Materializada: mv_ad_performance_metrics
-- Métricas de rendimiento de anuncios para dashboards
-- -----------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS gold.mv_ad_performance_metrics CASCADE;

CREATE MATERIALIZED VIEW gold.mv_ad_performance_metrics AS
SELECT 
    a.ad_id,
    'Anuncio ' || a.source_ad_id AS ad_name,
    ac.campaign_name,
    ac.advertiser,
    COUNT(ai.ad_impression_id) AS total_impressions,
    SUM(CASE WHEN ai.clicked = TRUE THEN 1 ELSE 0 END) AS total_clicks,
    CASE 
        WHEN COUNT(ai.ad_impression_id) > 0 
        THEN ROUND((SUM(CASE WHEN ai.clicked = TRUE THEN 1 ELSE 0 END)::NUMERIC / 
                    COUNT(ai.ad_impression_id)::NUMERIC) * 100, 2)
        ELSE 0
    END AS ctr_percentage,
    COUNT(DISTINCT ai.user_id) AS unique_users_reached,
    COUNT(DISTINCT ai.content_id) AS content_placements,
    a.duration_seconds AS ad_duration_seconds,
    MIN(ai.impression_ts) AS first_impression_date,
    MAX(ai.impression_ts) AS last_impression_date
FROM silver.ads a
LEFT JOIN silver.ad_campaigns ac ON a.campaign_id = ac.campaign_id
LEFT JOIN silver.ad_impressions ai ON a.ad_id = ai.ad_id
GROUP BY 
    a.ad_id, a.source_ad_id, ac.campaign_name, ac.advertiser, a.duration_seconds
ORDER BY total_impressions DESC NULLS LAST;

COMMENT ON MATERIALIZED VIEW gold.mv_ad_performance_metrics IS 'Métricas de rendimiento de anuncios con CTR para dashboards.';

-- Crear índice único en la MV
CREATE UNIQUE INDEX idx_mv_ad_perf_metrics 
ON gold.mv_ad_performance_metrics (ad_id);

-- -----------------------------------------------------------------------------
-- 17.4. Vista Materializada: mv_daily_platform_metrics (CORREGIDA)
-- Métricas diarias de la plataforma (KPIs generales)
-- -----------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS gold.mv_daily_platform_metrics CASCADE;

CREATE MATERIALIZED VIEW gold.mv_daily_platform_metrics AS
SELECT 
    streaming_metrics.metric_date,
    streaming_metrics.daily_active_users,
    streaming_metrics.total_sessions,
    streaming_metrics.unique_content_played,
    streaming_metrics.total_hours_streamed,
    streaming_metrics.avg_completion_rate,
    streaming_metrics.completed_sessions,
    COALESCE(ai_metrics.total_ad_impressions, 0) AS total_ad_impressions,
    COALESCE(ai_metrics.total_ad_clicks, 0) AS total_ad_clicks
FROM (
    -- Métricas de streaming
    SELECT 
        ss.start_ts::DATE AS metric_date,
        COUNT(DISTINCT ss.user_id) AS daily_active_users,
        COUNT(DISTINCT ss.session_id) AS total_sessions,
        COUNT(DISTINCT ss.content_id) AS unique_content_played,
        SUM(ss.duration_seconds) / 3600.0 AS total_hours_streamed,
        ROUND(AVG(ss.watched_pct), 2) AS avg_completion_rate,
        COUNT(DISTINCT CASE WHEN ss.watched_pct >= 90 THEN ss.session_id END) AS completed_sessions
    FROM silver.streaming_sessions ss
    GROUP BY ss.start_ts::DATE
) streaming_metrics
LEFT JOIN (
    -- Métricas de publicidad
    SELECT 
        ai.impression_ts::DATE AS metric_date,
        COUNT(*) AS total_ad_impressions,
        SUM(CASE WHEN ai.clicked THEN 1 ELSE 0 END) AS total_ad_clicks
    FROM silver.ad_impressions ai
    GROUP BY ai.impression_ts::DATE
) ai_metrics ON streaming_metrics.metric_date = ai_metrics.metric_date
ORDER BY streaming_metrics.metric_date DESC;

COMMENT ON MATERIALIZED VIEW gold.mv_daily_platform_metrics IS 'KPIs diarios de la plataforma para monitoreo ejecutivo.';

-- Crear índice único en la MV
CREATE UNIQUE INDEX idx_mv_daily_platform_metrics 
ON gold.mv_daily_platform_metrics (metric_date);

-- =============================================================================
-- PASO 18: FUNCIONES PARA REFRESCAR VISTAS MATERIALIZADAS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 18.1. Función para refrescar todas las vistas materializadas
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION gold.refresh_all_materialized_views()
RETURNS TEXT AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_result TEXT := '';
BEGIN
    v_start_time := CLOCK_TIMESTAMP();

    RAISE NOTICE 'Iniciando refresco de vistas materializadas...';

    -- Refrescar cada vista materializada
    REFRESH MATERIALIZED VIEW CONCURRENTLY gold.mv_monthly_user_activity;
    v_result := v_result || 'mv_monthly_user_activity refrescada. ';

    REFRESH MATERIALIZED VIEW CONCURRENTLY gold.mv_content_popularity_by_genre;
    v_result := v_result || 'mv_content_popularity_by_genre refrescada. ';

    REFRESH MATERIALIZED VIEW CONCURRENTLY gold.mv_ad_performance_metrics;
    v_result := v_result || 'mv_ad_performance_metrics refrescada. ';

    REFRESH MATERIALIZED VIEW CONCURRENTLY gold.mv_daily_platform_metrics;
    v_result := v_result || 'mv_daily_platform_metrics refrescada. ';

    v_result := v_result || 'Tiempo total: ' || 
                (EXTRACT(EPOCH FROM (CLOCK_TIMESTAMP() - v_start_time)))::TEXT || ' segundos.';

    RAISE NOTICE 'Refresco completado: %', v_result;

    RETURN v_result;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gold.refresh_all_materialized_views IS 'Refresca todas las vistas materializadas de la capa Gold.';

-- =============================================================================
-- PASO 19: ROLES Y PERMISOS DE SEGURIDAD
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 19.1. Crear roles de base de datos
-- -----------------------------------------------------------------------------

-- Revocar permisos y eliminar rol ETL si existe
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'etl_process_role') THEN
        REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze FROM etl_process_role;
        REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bronze FROM etl_process_role;
        REVOKE ALL PRIVILEGES ON SCHEMA bronze FROM etl_process_role;
        
        REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver FROM etl_process_role;
        REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA silver FROM etl_process_role;
        REVOKE ALL PRIVILEGES ON SCHEMA silver FROM etl_process_role;
        
        DROP ROLE etl_process_role;
    END IF;
END $$;

CREATE ROLE etl_process_role;
COMMENT ON ROLE etl_process_role IS 'Rol para procesos de ETL con acceso a Bronze y Silver.';

-- Revocar permisos y eliminar rol BI si existe
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bi_analyst_role') THEN
        REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold FROM bi_analyst_role;
        REVOKE ALL PRIVILEGES ON SCHEMA gold FROM bi_analyst_role;
        
        DROP ROLE bi_analyst_role;
    END IF;
END $$;

CREATE ROLE bi_analyst_role;
COMMENT ON ROLE bi_analyst_role IS 'Rol para analistas de BI con acceso de lectura a Gold.';

-- Revocar permisos y eliminar rol API si existe
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_consumer_role') THEN
        REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold FROM api_consumer_role;
        REVOKE ALL PRIVILEGES ON SCHEMA gold FROM api_consumer_role;
        
        DROP ROLE api_consumer_role;
    END IF;
END $$;

CREATE ROLE api_consumer_role;
COMMENT ON ROLE api_consumer_role IS 'Rol para APIs con acceso de lectura a vistas de Gold.';

-- Rol para procesos ETL
DROP ROLE IF EXISTS etl_process_role;
CREATE ROLE etl_process_role;
COMMENT ON ROLE etl_process_role IS 'Rol para procesos de ETL con acceso a Bronze y Silver.';

-- Rol para analistas de BI
DROP ROLE IF EXISTS bi_analyst_role;
CREATE ROLE bi_analyst_role;
COMMENT ON ROLE bi_analyst_role IS 'Rol para analistas de BI con acceso de lectura a Gold.';

-- Rol para consumidores de API
DROP ROLE IF EXISTS api_consumer_role;
CREATE ROLE api_consumer_role;
COMMENT ON ROLE api_consumer_role IS 'Rol para APIs con acceso de lectura a vistas de Gold.';

-- -----------------------------------------------------------------------------
-- 19.2. Asignar permisos a roles
-- -----------------------------------------------------------------------------

-- Permisos para ETL (lectura en Bronze, escritura en Silver)
GRANT USAGE ON SCHEMA bronze TO etl_process_role;
GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO etl_process_role;

GRANT USAGE ON SCHEMA silver TO etl_process_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA silver TO etl_process_role;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA silver TO etl_process_role;

-- Permisos para analistas de BI (solo lectura en Gold)
GRANT USAGE ON SCHEMA gold TO bi_analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO bi_analyst_role;

-- Permisos para API (solo lectura de vistas en Gold)
GRANT USAGE ON SCHEMA gold TO api_consumer_role;
GRANT SELECT ON gold.vw_user_profile_details TO api_consumer_role;
GRANT SELECT ON gold.vw_content_catalog_summary TO api_consumer_role;
GRANT SELECT ON gold.vw_user_playback_history TO api_consumer_role;
GRANT SELECT ON gold.vw_playlist_details TO api_consumer_role;
GRANT SELECT ON gold.vw_ad_performance_summary TO api_consumer_role;

-- =============================================================================
-- VERIFICACIÓN FINAL Y RESUMEN
-- =============================================================================

-- Verificar triggers creados
SELECT 
    trigger_schema,
    trigger_name,
    event_object_table,
    action_timing,
    event_manipulation
FROM information_schema.triggers
WHERE trigger_schema IN ('silver', 'gold', 'audit')
ORDER BY trigger_schema, event_object_table, trigger_name;

-- Verificar vistas creadas
SELECT 
    table_schema,
    table_name,
    'VIEW' AS table_type
FROM information_schema.views
WHERE table_schema = 'gold'
ORDER BY table_name;

-- Verificar vistas materializadas
SELECT 
    schemaname,
    matviewname,
    hasindexes
FROM pg_matviews
WHERE schemaname = 'gold'
ORDER BY matviewname;

-- Probar refresco de vistas materializadas
SELECT gold.refresh_all_materialized_views();

-- Resumen de objetos creados
SELECT 'Esquemas' AS tipo, COUNT(*) AS cantidad FROM information_schema.schemata 
    WHERE schema_name IN ('bronze', 'silver', 'gold', 'audit')
UNION ALL
SELECT 'Tablas Bronze', COUNT(*) FROM information_schema.tables 
    WHERE table_schema = 'bronze' AND table_type = 'BASE TABLE'
UNION ALL
SELECT 'Tablas Silver', COUNT(*) FROM information_schema.tables 
    WHERE table_schema = 'silver' AND table_type = 'BASE TABLE'
UNION ALL
SELECT 'Tablas Gold', COUNT(*) FROM information_schema.tables 
    WHERE table_schema = 'gold' AND table_type = 'BASE TABLE'
UNION ALL
SELECT 'Tablas Audit', COUNT(*) FROM information_schema.tables 
    WHERE table_schema = 'audit' AND table_type = 'BASE TABLE'
UNION ALL
SELECT 'Vistas', COUNT(*) FROM information_schema.views 
    WHERE table_schema = 'gold'
UNION ALL
SELECT 'Vistas Materializadas', COUNT(*) FROM pg_matviews 
    WHERE schemaname = 'gold'
UNION ALL
SELECT 'Triggers', COUNT(*) FROM information_schema.triggers 
    WHERE trigger_schema IN ('silver', 'gold', 'audit')
UNION ALL
SELECT 'Funciones', COUNT(*) FROM information_schema.routines 
    WHERE routine_schema IN ('silver', 'gold', 'audit')
UNION ALL
SELECT 'Roles', COUNT(*) FROM pg_roles 
    WHERE rolname IN ('etl_process_role', 'bi_analyst_role', 'api_consumer_role');
