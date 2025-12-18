-- =============================================================================
-- PARTE 1: CONSULTAS A VISTAS DE NEGOCIO
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1.1. Perfil completo de un usuario específico
-- Muestra información integrada de usuario, suscripción y actividad
-- -----------------------------------------------------------------------------
SELECT 
    user_id,
    full_name,
    email,
    country_name,
    subscription_plan,
    plan_price,
    subscription_status,
    registration_date,
    last_activity_date,
    total_sessions,
    ROUND(total_watch_time_minutes / 60.0, 2) AS total_watch_time_hours
FROM gold.vw_user_profile_details
WHERE user_id = 1001
ORDER BY last_activity_date DESC;

-- -----------------------------------------------------------------------------
-- 1.2. Top 10 contenidos más populares
-- Ranking de contenido por número de vistas
-- -----------------------------------------------------------------------------
SELECT 
    title,
    content_type,
    genres,
    total_views,
    unique_viewers,
    avg_completion_rate,
    ROUND(total_watch_time_minutes / 60.0, 2) AS total_watch_hours
FROM gold.vw_content_catalog_summary
WHERE total_views > 0
ORDER BY total_views DESC, unique_viewers DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 1.3. Historial de reproducciones de usuario por tipo de contenido
-- Análisis de hábitos de consumo
-- -----------------------------------------------------------------------------
SELECT 
    user_name,
    content_type,
    COUNT(*) AS total_reproducciones,
    ROUND(AVG(completion_percentage), 2) AS avg_completion_rate,
    SUM(session_duration_minutes) AS total_tiempo_minutos
FROM gold.vw_user_playback_history
WHERE user_id = 1001
GROUP BY user_name, content_type
ORDER BY total_reproducciones DESC;

-- -----------------------------------------------------------------------------
-- 1.4. Análisis de dispositivos más utilizados
-- Distribución de reproducciones por dispositivo
-- -----------------------------------------------------------------------------
SELECT 
    device_type,
    COUNT(DISTINCT user_id) AS usuarios_unicos,
    COUNT(*) AS total_sesiones,
    ROUND(AVG(completion_percentage), 2) AS avg_completion_rate
FROM gold.vw_user_playback_history
GROUP BY device_type
ORDER BY total_sesiones DESC;

-- -----------------------------------------------------------------------------
-- 1.5. Rendimiento de campañas publicitarias
-- Top campañas por CTR (Click-Through Rate)
-- -----------------------------------------------------------------------------
SELECT 
    campaign_name,
    advertiser,
    total_ads,
    total_impressions,
    total_clicks,
    click_through_rate_pct,
    unique_users_reached,
    ROUND(total_ad_time_seconds / 60.0, 2) AS total_ad_time_minutes
FROM gold.vw_ad_performance_summary
WHERE total_impressions > 0
ORDER BY click_through_rate_pct DESC, total_impressions DESC;

-- -----------------------------------------------------------------------------
-- 1.6. Playlists más largas creadas por usuarios
-- Análisis de engagement de usuarios
-- -----------------------------------------------------------------------------
SELECT 
    user_name,
    playlist_name,
    total_items,
    ROUND(total_duration_minutes / 60.0, 2) AS total_duration_hours,
    playlist_created_at
FROM gold.vw_playlist_details
WHERE total_items > 0
ORDER BY total_items DESC, total_duration_minutes DESC
LIMIT 10;

-- =============================================================================
-- PARTE 2: CONSULTAS A VISTAS MATERIALIZADAS (ANÁLISIS AGREGADO)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 2.1. Usuarios más activos del mes actual
-- Top 10 usuarios por horas consumidas en el mes
-- -----------------------------------------------------------------------------
SELECT 
    full_name,
    email,
    country_name,
    plan_name,
    activity_month,
    total_sessions,
    unique_content_watched,
    ROUND(total_hours_watched, 2) AS horas_vistas,
    avg_completion_rate,
    completed_content_count
FROM gold.mv_monthly_user_activity
WHERE activity_month = DATE_TRUNC('month', CURRENT_DATE)::DATE
ORDER BY total_hours_watched DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 2.2. Tendencia de actividad de usuario específico (últimos 6 meses)
-- Análisis temporal de engagement
-- -----------------------------------------------------------------------------
SELECT 
    activity_month,
    total_sessions,
    unique_content_watched,
    ROUND(total_hours_watched, 2) AS horas_vistas,
    avg_completion_rate,
    completed_content_count
FROM gold.mv_monthly_user_activity
WHERE user_id = 1001
  AND activity_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '6 months')::DATE
ORDER BY activity_month DESC;

-- -----------------------------------------------------------------------------
-- 2.3. Géneros más populares por tipo de contenido
-- Preferencias de usuarios por categoría
-- -----------------------------------------------------------------------------
SELECT 
    genre_name,
    content_type,
    total_content_items,
    total_views,
    unique_viewers,
    ROUND(total_hours_watched, 2) AS horas_totales,
    avg_completion_rate,
    ROUND((total_views::NUMERIC / NULLIF(total_content_items, 0)), 2) AS avg_views_per_content
FROM gold.mv_content_popularity_by_genre
WHERE total_views > 0
ORDER BY total_views DESC, total_hours_watched DESC;

-- -----------------------------------------------------------------------------
-- 2.4. Comparación de popularidad: Video vs Música
-- Análisis agregado por tipo de contenido
-- -----------------------------------------------------------------------------
SELECT 
    content_type,
    SUM(total_content_items) AS total_items,
    SUM(total_views) AS total_views,
    SUM(unique_viewers) AS usuarios_unicos,
    ROUND(SUM(total_hours_watched), 2) AS horas_totales,
    ROUND(AVG(avg_completion_rate), 2) AS avg_completion
FROM gold.mv_content_popularity_by_genre
GROUP BY content_type
ORDER BY total_views DESC;

-- -----------------------------------------------------------------------------
-- 2.5. Anuncios más efectivos (mejor CTR)
-- Top 10 anuncios por tasa de clicks
-- -----------------------------------------------------------------------------
SELECT 
    ad_name,
    campaign_name,
    advertiser,
    total_impressions,
    total_clicks,
    ctr_percentage,
    unique_users_reached,
    content_placements,
    first_impression_date,
    last_impression_date
FROM gold.mv_ad_performance_metrics
WHERE total_impressions > 0
ORDER BY ctr_percentage DESC, total_impressions DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 2.6. Métricas de la plataforma - Últimos 7 días
-- KPIs diarios para monitoreo ejecutivo
-- -----------------------------------------------------------------------------
SELECT 
    metric_date,
    daily_active_users AS usuarios_activos,
    total_sessions AS sesiones_totales,
    unique_content_played AS contenido_unico_reproducido,
    ROUND(total_hours_streamed, 2) AS horas_streaming,
    avg_completion_rate AS tasa_completado_promedio,
    completed_sessions AS sesiones_completadas,
    total_ad_impressions AS impresiones_publicidad,
    total_ad_clicks AS clicks_publicidad,
    CASE 
        WHEN total_ad_impressions > 0 
        THEN ROUND((total_ad_clicks::NUMERIC / total_ad_impressions::NUMERIC) * 100, 2)
        ELSE 0
    END AS ctr_diario
FROM gold.mv_daily_platform_metrics
WHERE metric_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY metric_date DESC;

-- -----------------------------------------------------------------------------
-- 2.7. Crecimiento de usuarios activos (comparación semanal)
-- Tendencia de crecimiento de la plataforma
-- -----------------------------------------------------------------------------
SELECT 
    DATE_TRUNC('week', metric_date)::DATE AS semana,
    AVG(daily_active_users) AS promedio_usuarios_diarios,
    SUM(total_sessions) AS total_sesiones_semana,
    ROUND(SUM(total_hours_streamed), 2) AS total_horas_semana
FROM gold.mv_daily_platform_metrics
WHERE metric_date >= CURRENT_DATE - INTERVAL '4 weeks'
GROUP BY DATE_TRUNC('week', metric_date)::DATE
ORDER BY semana DESC;

-- =============================================================================
-- PARTE 3: CONSULTAS ANALÍTICAS AVANZADAS (MODELO DIMENSIONAL)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 3.1. Análisis de actividad por hora del día
-- Patrones de uso de la plataforma
-- -----------------------------------------------------------------------------
SELECT 
    t.hour,
    t.time_period,
    COUNT(DISTINCT fa.user_key) AS usuarios_unicos,
    SUM(fa.total_sessions) AS sesiones_totales,
    ROUND(SUM(fa.total_duration_minutes) / 60.0, 2) AS horas_totales,
    ROUND(AVG(fa.avg_completion_rate), 2) AS tasa_completado_promedio
FROM gold.fact_user_activity fa
JOIN gold.dim_time_of_day t ON fa.time_key = t.time_key
GROUP BY t.hour, t.time_period
ORDER BY t.hour;

-- -----------------------------------------------------------------------------
-- 3.2. Contenido más popular por país
-- Top 3 contenidos por país
-- -----------------------------------------------------------------------------
WITH ranked_content AS (
    SELECT 
        u.country_name,
        c.title,
        c.content_type,
        SUM(fa.total_sessions) AS total_views,
        ROW_NUMBER() OVER (PARTITION BY u.country_name ORDER BY SUM(fa.total_sessions) DESC) AS rank
    FROM gold.fact_user_activity fa
    JOIN gold.dim_users u ON fa.user_key = u.user_key
    JOIN gold.dim_content c ON fa.content_key = c.content_key
    GROUP BY u.country_name, c.title, c.content_type
)
SELECT 
    country_name,
    title,
    content_type,
    total_views
FROM ranked_content
WHERE rank <= 3
ORDER BY country_name, rank;

-- -----------------------------------------------------------------------------
-- 3.3. Análisis de fin de semana vs días laborables
-- Diferencias en patrones de consumo
-- -----------------------------------------------------------------------------
SELECT 
    d.is_weekend,
    CASE WHEN d.is_weekend THEN 'Fin de Semana' ELSE 'Días Laborables' END AS periodo,
    COUNT(DISTINCT fa.user_key) AS usuarios_unicos,
    SUM(fa.total_sessions) AS sesiones_totales,
    ROUND(AVG(fa.total_duration_minutes) / 60.0, 2) AS promedio_horas_por_usuario,
    ROUND(AVG(fa.avg_completion_rate), 2) AS tasa_completado_promedio
FROM gold.fact_user_activity fa
JOIN gold.dim_date d ON fa.date_key = d.date_key
GROUP BY d.is_weekend
ORDER BY d.is_weekend;

-- -----------------------------------------------------------------------------
-- 3.4. Rendimiento de publicidad por género de contenido
-- CTR de anuncios según contexto de reproducción
-- -----------------------------------------------------------------------------
SELECT 
    c.primary_genre,
    SUM(ap.total_impressions) AS impresiones_totales,
    SUM(ap.total_clicks) AS clicks_totales,
    ROUND((SUM(ap.total_clicks)::NUMERIC / NULLIF(SUM(ap.total_impressions), 0)::NUMERIC) * 100, 2) AS ctr_percentage,
    COUNT(DISTINCT ap.ad_id) AS anuncios_diferentes
FROM gold.fact_ad_performance ap
JOIN gold.dim_content c ON ap.content_key = c.content_key
GROUP BY c.primary_genre
HAVING SUM(ap.total_impressions) > 0
ORDER BY ctr_percentage DESC, impresiones_totales DESC;

-- -----------------------------------------------------------------------------
-- 3.5. Usuarios premium vs con publicidad (comparación)
-- Análisis de comportamiento por tipo de suscripción
-- -----------------------------------------------------------------------------
SELECT 
    u.current_plan_name,
    COUNT(DISTINCT u.user_key) AS total_usuarios,
    ROUND(AVG(fa.total_duration_minutes) / 60.0, 2) AS promedio_horas_usuario,
    ROUND(AVG(fa.total_sessions), 2) AS promedio_sesiones_usuario,
    ROUND(AVG(fa.avg_completion_rate), 2) AS tasa_completado_promedio
FROM gold.dim_users u
LEFT JOIN gold.fact_user_activity fa ON u.user_key = fa.user_key
WHERE u.current_plan_name IS NOT NULL
GROUP BY u.current_plan_name
ORDER BY promedio_horas_usuario DESC;

-- -----------------------------------------------------------------------------
-- 3.6. Popularidad de contenido por trimestre
-- Tendencia temporal de reproducciones
-- -----------------------------------------------------------------------------
SELECT 
    d.year,
    d.quarter AS trimestre,
    COUNT(DISTINCT cp.content_key) AS contenidos_reproducidos,
    SUM(cp.total_views) AS vistas_totales,
    SUM(cp.unique_users) AS usuarios_unicos,
    ROUND(SUM(cp.total_watch_time_minutes) / 60.0, 2) AS horas_totales
FROM gold.fact_content_popularity cp
JOIN gold.dim_date d ON cp.date_key = d.date_key
GROUP BY d.year, d.quarter
ORDER BY d.year DESC, d.quarter DESC;

-- =============================================================================
-- PARTE 4: AUDITORÍA Y CALIDAD DE DATOS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 4.1. Últimos cambios en usuarios
-- Auditoría de modificaciones recientes
-- -----------------------------------------------------------------------------
SELECT 
    user_id,
    operation_type,
    changed_by,
    changed_at,
    CASE 
        WHEN operation_type = 'UPDATE' THEN 
            (new_data->>'full_name') || ' (antes: ' || (old_data->>'full_name') || ')'
        WHEN operation_type = 'INSERT' THEN 
            (new_data->>'full_name')
        ELSE 
            (old_data->>'full_name')
    END AS usuario_afectado
FROM audit.history_users
ORDER BY changed_at DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 4.2. Cambios en suscripciones (últimas 24 horas)
-- Monitoreo de actualizaciones de planes
-- -----------------------------------------------------------------------------
SELECT 
    subscription_id,
    operation_type,
    old_data->>'payment_status' AS estado_anterior,
    new_data->>'payment_status' AS estado_nuevo,
    changed_by,
    changed_at
FROM audit.history_subscriptions
WHERE changed_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
ORDER BY changed_at DESC;

-- -----------------------------------------------------------------------------
-- 4.3. Validación de integridad: Contenido sin género asignado
-- Control de calidad de datos
-- -----------------------------------------------------------------------------
SELECT 
    c.content_id,
    c.title,
    c.content_type,
    c.release_year
FROM silver.content c
LEFT JOIN silver.content_genres cg ON c.content_id = cg.content_id
WHERE cg.genre_id IS NULL;

-- -----------------------------------------------------------------------------
-- 4.4. Sesiones con duración anómala (muy corta o muy larga)
-- Detección de posibles errores de datos
-- -----------------------------------------------------------------------------
SELECT 
    session_id,
    user_id,
    content_id,
    start_ts,
    end_ts,
    ROUND(duration_seconds / 60.0, 2) AS duracion_minutos,
    watched_pct
FROM silver.streaming_sessions
WHERE duration_seconds < 10 OR duration_seconds > 18000 -- < 10 seg o > 5 horas
ORDER BY duration_seconds DESC;

-- =============================================================================
-- PARTE 5: QUERIES PARA REFRESCAR Y MANTENER EL SISTEMA
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 5.1. Refrescar todas las vistas materializadas
-- Ejecutar periódicamente para actualizar métricas
-- -----------------------------------------------------------------------------
SELECT gold.refresh_all_materialized_views();

-- -----------------------------------------------------------------------------
-- 5.2. Estadísticas del sistema completo
-- Resumen ejecutivo de todos los objetos creados
-- -----------------------------------------------------------------------------
SELECT 
    'Total Usuarios' AS metrica, 
    COUNT(*)::TEXT AS valor 
FROM silver.users
UNION ALL
SELECT 'Total Contenido', COUNT(*)::TEXT FROM silver.content
UNION ALL
SELECT 'Total Sesiones Streaming', COUNT(*)::TEXT FROM silver.streaming_sessions
UNION ALL
SELECT 'Total Impresiones Publicidad', COUNT(*)::TEXT FROM silver.ad_impressions
UNION ALL
SELECT 'Total Playlists', COUNT(*)::TEXT FROM silver.playlists
UNION ALL
SELECT 'Vistas Materializadas', COUNT(*)::TEXT FROM pg_matviews WHERE schemaname = 'gold'
UNION ALL
SELECT 'Vistas Normales', COUNT(*)::TEXT FROM information_schema.views WHERE table_schema = 'gold'
UNION ALL
SELECT 'Triggers Activos', COUNT(*)::TEXT FROM information_schema.triggers 
    WHERE trigger_schema IN ('silver', 'gold', 'audit');
