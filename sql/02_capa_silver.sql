-- =============================================================================
-- PARTE 2: CAPA SILVER - DATOS LIMPIOS Y NORMALIZADOS
-- =============================================================================

-- =============================================================================
-- PASO 4: SECUENCIAS PARA IDS EN SILVER
-- =============================================================================

-- Usuarios, países, planes, contenido, géneros, artistas, playlists, etc.

CREATE SEQUENCE IF NOT EXISTS silver.user_seq START 1001;
COMMENT ON SEQUENCE silver.user_seq IS 'Secuencia para IDs de usuarios en capa Silver.';

CREATE SEQUENCE IF NOT EXISTS silver.country_seq START 1;
COMMENT ON SEQUENCE silver.country_seq IS 'Secuencia para IDs de países.';

CREATE SEQUENCE IF NOT EXISTS silver.plan_seq START 1;
COMMENT ON SEQUENCE silver.plan_seq IS 'Secuencia para IDs de planes de suscripción.';

CREATE SEQUENCE IF NOT EXISTS silver.subscription_seq START 2001;
COMMENT ON SEQUENCE silver.subscription_seq IS 'Secuencia para IDs de suscripciones.';

CREATE SEQUENCE IF NOT EXISTS silver.content_seq START 3001;
COMMENT ON SEQUENCE silver.content_seq IS 'Secuencia para IDs de contenido.';

CREATE SEQUENCE IF NOT EXISTS silver.genre_seq START 4001;
COMMENT ON SEQUENCE silver.genre_seq IS 'Secuencia para IDs de géneros.';

CREATE SEQUENCE IF NOT EXISTS silver.artist_seq START 5001;
COMMENT ON SEQUENCE silver.artist_seq IS 'Secuencia para IDs de artistas/directores.';

CREATE SEQUENCE IF NOT EXISTS silver.content_genre_seq START 6001;
COMMENT ON SEQUENCE silver.content_genre_seq IS 'Secuencia para IDs de relaciones contenido-género.';

CREATE SEQUENCE IF NOT EXISTS silver.content_artist_seq START 7001;
COMMENT ON SEQUENCE silver.content_artist_seq IS 'Secuencia para IDs de relaciones contenido-artista.';

CREATE SEQUENCE IF NOT EXISTS silver.device_seq START 8001;
COMMENT ON SEQUENCE silver.device_seq IS 'Secuencia para IDs de dispositivos.';

CREATE SEQUENCE IF NOT EXISTS silver.session_seq START 9001;
COMMENT ON SEQUENCE silver.session_seq IS 'Secuencia para IDs de sesiones de streaming.';

CREATE SEQUENCE IF NOT EXISTS silver.playlist_seq START 10001;
COMMENT ON SEQUENCE silver.playlist_seq IS 'Secuencia para IDs de playlists.';

CREATE SEQUENCE IF NOT EXISTS silver.playlist_item_seq START 11001;
COMMENT ON SEQUENCE silver.playlist_item_seq IS 'Secuencia para IDs de items de playlist.';

CREATE SEQUENCE IF NOT EXISTS silver.ad_campaign_seq START 12001;
COMMENT ON SEQUENCE silver.ad_campaign_seq IS 'Secuencia para IDs de campañas publicitarias.';

CREATE SEQUENCE IF NOT EXISTS silver.ad_seq START 13001;
COMMENT ON SEQUENCE silver.ad_seq IS 'Secuencia para IDs de anuncios publicitarios.';

CREATE SEQUENCE IF NOT EXISTS silver.ad_impression_seq START 14001;
COMMENT ON SEQUENCE silver.ad_impression_seq IS 'Secuencia para IDs de impresiones de anuncios.';

-- =============================================================================
-- PASO 5: TABLAS MAESTRAS EN CAPA SILVER
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 5.1. Tabla: silver.countries
-- Normaliza países desde los registros de usuarios
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.countries (
    country_id     INT DEFAULT nextval('silver.country_seq') PRIMARY KEY,
    country_name   VARCHAR(150) NOT NULL,
    iso_code       VARCHAR(10),
    region         VARCHAR(100),
    created_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE silver.countries IS 'Catálogo de países normalizado para usuarios.';
COMMENT ON COLUMN silver.countries.country_name IS 'Nombre del país tal como se utilizará en Silver.';

-- ---------------------------------------------------------------------------
-- 5.2. Tabla: silver.subscription_plans
-- Catálogo de planes de suscripción
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.subscription_plans (
    plan_id        INT DEFAULT nextval('silver.plan_seq') PRIMARY KEY,
    plan_name      VARCHAR(100) NOT NULL,   -- Premium, Con Publicidad, etc.
    monthly_price  NUMERIC(10,2) NOT NULL DEFAULT 0,
    description    TEXT,
    created_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE silver.subscription_plans IS 'Catálogo de planes de suscripción.';
COMMENT ON COLUMN silver.subscription_plans.plan_name IS 'Nombre del plan de suscripción.';

-- ---------------------------------------------------------------------------
-- 5.3. Tabla: silver.users
-- Usuarios normalizados con referencias a countries y planes
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.users (
    user_id            INT DEFAULT nextval('silver.user_seq') PRIMARY KEY,
    source_user_id     VARCHAR(100) NOT NULL,    -- user_id original de Bronze
    full_name          VARCHAR(255) NOT NULL,
    email              VARCHAR(255) NOT NULL UNIQUE,
    country_id         INT,
    active             BOOLEAN DEFAULT TRUE,
    registration_ts    TIMESTAMP WITH TIME ZONE,
    current_plan_id    INT,
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_users_country
        FOREIGN KEY (country_id) REFERENCES silver.countries(country_id),
    CONSTRAINT fk_users_plan
        FOREIGN KEY (current_plan_id) REFERENCES silver.subscription_plans(plan_id)
);

COMMENT ON TABLE silver.users IS 'Usuarios normalizados, derivados de bronze.user_registrations.';
COMMENT ON COLUMN silver.users.source_user_id IS 'Identificador original de la fuente en Bronze.';

-- ---------------------------------------------------------------------------
-- 5.4. Tabla: silver.subscriptions
-- Suscripciones limpias por usuario
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.subscriptions (
    subscription_id    INT DEFAULT nextval('silver.subscription_seq') PRIMARY KEY,
    source_subscription_id VARCHAR(100) NOT NULL, -- ID original de Bronze
    user_id            INT NOT NULL,
    plan_id            INT NOT NULL,
    start_date         DATE NOT NULL,
    end_date           DATE,
    payment_status     VARCHAR(50) NOT NULL,      -- active, pending, expired...
    payment_amount     NUMERIC(10,2),
    currency           VARCHAR(10),
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_subscriptions_user
        FOREIGN KEY (user_id) REFERENCES silver.users(user_id),
    CONSTRAINT fk_subscriptions_plan
        FOREIGN KEY (plan_id) REFERENCES silver.subscription_plans(plan_id)
);

COMMENT ON TABLE silver.subscriptions IS 'Suscripciones limpias por usuario, normalizadas.';
COMMENT ON COLUMN silver.subscriptions.payment_status IS 'Estado del pago ya estandarizado.';

-- ---------------------------------------------------------------------------
-- 5.5. Tabla: silver.content
-- Catálogo de contenido multimedia normalizado
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.content (
    content_id         INT DEFAULT nextval('silver.content_seq') PRIMARY KEY,
    source_content_id  VARCHAR(100) NOT NULL,   -- content_id original de Bronze
    title              VARCHAR(500) NOT NULL,
    content_type       VARCHAR(50) NOT NULL,    -- video, music
    release_year       INT,
    duration_minutes   NUMERIC(10,2),
    description        TEXT,
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE silver.content IS 'Catálogo de contenido multimedia normalizado.';
COMMENT ON COLUMN silver.content.duration_minutes IS 'Duración estándar en minutos.';

-- ---------------------------------------------------------------------------
-- 5.6. Tabla: silver.genres
-- Catálogo de géneros (puede ser jerárquico)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.genres (
    genre_id           INT DEFAULT nextval('silver.genre_seq') PRIMARY KEY,
    genre_name         VARCHAR(200) NOT NULL,
    parent_genre_id    INT,
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_genres_parent
        FOREIGN KEY (parent_genre_id) REFERENCES silver.genres(genre_id)
);

COMMENT ON TABLE silver.genres IS 'Catálogo de géneros de contenido.';
COMMENT ON COLUMN silver.genres.parent_genre_id IS 'Permite crear jerarquías de géneros.';

-- ---------------------------------------------------------------------------
-- 5.7. Tabla: silver.content_genres
-- Relación N a N entre contenido y géneros
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.content_genres (
    content_genre_id   INT DEFAULT nextval('silver.content_genre_seq') PRIMARY KEY,
    content_id         INT NOT NULL,
    genre_id           INT NOT NULL,
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_content_genres_content
        FOREIGN KEY (content_id) REFERENCES silver.content(content_id),
    CONSTRAINT fk_content_genres_genre
        FOREIGN KEY (genre_id) REFERENCES silver.genres(genre_id)
);

COMMENT ON TABLE silver.content_genres IS 'Relación entre contenido y géneros normalizados.';

-- ---------------------------------------------------------------------------
-- 5.8. Tabla: silver.artists_directors
-- Personas asociadas al contenido (artistas, directores, etc.)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.artists_directors (
    artist_id          INT DEFAULT nextval('silver.artist_seq') PRIMARY KEY,
    name               VARCHAR(300) NOT NULL,
    role_type          VARCHAR(50),         -- director, artista, actor, etc.
    biography          TEXT,
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE silver.artists_directors IS 'Catálogo de artistas y directores.';

-- ---------------------------------------------------------------------------
-- 5.9. Tabla: silver.content_artists
-- Relación N a N entre contenido y artistas/directores
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.content_artists (
    content_artist_id  INT DEFAULT nextval('silver.content_artist_seq') PRIMARY KEY,
    content_id         INT NOT NULL,
    artist_id          INT NOT NULL,
    role_description   VARCHAR(100),
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_content_artists_content
        FOREIGN KEY (content_id) REFERENCES silver.content(content_id),
    CONSTRAINT fk_content_artists_artist
        FOREIGN KEY (artist_id) REFERENCES silver.artists_directors(artist_id)
);

COMMENT ON TABLE silver.content_artists IS 'Relación entre contenido y artistas/directores.';

-- ---------------------------------------------------------------------------
-- 5.10. Tabla: silver.devices
-- Catálogo de tipos de dispositivos
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.devices (
    device_id          INT DEFAULT nextval('silver.device_seq') PRIMARY KEY,
    device_type        VARCHAR(100) NOT NULL,    -- Mobile, Smart TV, Tablet, Web Browser
    operating_system   VARCHAR(100),
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE silver.devices IS 'Catálogo de dispositivos normalizados.';

-- ---------------------------------------------------------------------------
-- 5.11. Tabla: silver.streaming_sessions
-- Sesiones de streaming limpias, derivadas de bronze.raw_streaming_logs
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.streaming_sessions (
    session_id         INT DEFAULT nextval('silver.session_seq') PRIMARY KEY,
    user_id            INT NOT NULL,
    content_id         INT NOT NULL,
    device_id          INT,
    start_ts           TIMESTAMP WITH TIME ZONE NOT NULL,
    end_ts             TIMESTAMP WITH TIME ZONE,
    duration_seconds   NUMERIC(18,2),
    watched_pct        NUMERIC(5,2),
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_streaming_sessions_user
        FOREIGN KEY (user_id) REFERENCES silver.users(user_id),
    CONSTRAINT fk_streaming_sessions_content
        FOREIGN KEY (content_id) REFERENCES silver.content(content_id),
    CONSTRAINT fk_streaming_sessions_device
        FOREIGN KEY (device_id) REFERENCES silver.devices(device_id)
);

COMMENT ON TABLE silver.streaming_sessions IS 'Sesiones de streaming limpias con duraciones calculadas.';

-- ---------------------------------------------------------------------------
-- 5.12. Tabla: silver.playlists
-- Listas de reproducción creadas por los usuarios
-- (Para el proyecto se puede poblar con datos sintéticos luego)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.playlists (
    playlist_id        INT DEFAULT nextval('silver.playlist_seq') PRIMARY KEY,
    user_id            INT NOT NULL,
    name               VARCHAR(255) NOT NULL,
    description        TEXT,
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_playlists_user
        FOREIGN KEY (user_id) REFERENCES silver.users(user_id)
);

COMMENT ON TABLE silver.playlists IS 'Playlists creadas por usuarios.';

-- ---------------------------------------------------------------------------
-- 5.13. Tabla: silver.playlist_items
-- Items pertenecientes a cada playlist
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.playlist_items (
    playlist_item_id   INT DEFAULT nextval('silver.playlist_item_seq') PRIMARY KEY,
    playlist_id        INT NOT NULL,
    content_id         INT NOT NULL,
    position           INT NOT NULL,
    added_at           TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_playlist_items_playlist
        FOREIGN KEY (playlist_id) REFERENCES silver.playlists(playlist_id),
    CONSTRAINT fk_playlist_items_content
        FOREIGN KEY (content_id) REFERENCES silver.content(content_id)
);

COMMENT ON TABLE silver.playlist_items IS 'Elementos (contenido) asociados a playlists.';

-- ---------------------------------------------------------------------------
-- 5.14. Tabla: silver.ad_campaigns
-- Campañas publicitarias
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.ad_campaigns (
    campaign_id        INT DEFAULT nextval('silver.ad_campaign_seq') PRIMARY KEY,
    campaign_name      VARCHAR(255) NOT NULL,
    advertiser         VARCHAR(255),
    start_date         DATE,
    end_date           DATE,
    budget_amount      NUMERIC(14,2),
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE silver.ad_campaigns IS 'Campañas publicitarias de la plataforma.';

-- ---------------------------------------------------------------------------
-- 5.15. Tabla: silver.ads
-- Anuncios individuales dentro de una campaña
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.ads (
    ad_id              INT DEFAULT nextval('silver.ad_seq') PRIMARY KEY,
    source_ad_id       VARCHAR(100),          -- ad_id original de Bronze
    campaign_id        INT,
    title              VARCHAR(255),
    duration_seconds   NUMERIC(10,2),
    target_genre_id    INT,                   -- segmentación básica por género
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_ads_campaign
        FOREIGN KEY (campaign_id) REFERENCES silver.ad_campaigns(campaign_id),
    CONSTRAINT fk_ads_genre
        FOREIGN KEY (target_genre_id) REFERENCES silver.genres(genre_id)
);

COMMENT ON TABLE silver.ads IS 'Anuncios publicitarios normalizados.';

-- ---------------------------------------------------------------------------
-- 5.16. Tabla: silver.ad_impressions
-- Impresiones de anuncios limpias, derivadas de bronze.raw_ad_impressions
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.ad_impressions (
    ad_impression_id   INT DEFAULT nextval('silver.ad_impression_seq') PRIMARY KEY,
    ad_id              INT,
    user_id            INT,
    content_id         INT,
    session_id         INT,
    impression_ts      TIMESTAMP WITH TIME ZONE NOT NULL,
    ad_placement       VARCHAR(50),
    clicked            BOOLEAN,
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_ad_impressions_ad
        FOREIGN KEY (ad_id) REFERENCES silver.ads(ad_id),
    CONSTRAINT fk_ad_impressions_user
        FOREIGN KEY (user_id) REFERENCES silver.users(user_id),
    CONSTRAINT fk_ad_impressions_content
        FOREIGN KEY (content_id) REFERENCES silver.content(content_id),
    CONSTRAINT fk_ad_impressions_session
        FOREIGN KEY (session_id) REFERENCES silver.streaming_sessions(session_id)
);

COMMENT ON TABLE silver.ad_impressions IS 'Impresiones de anuncios normalizadas y limpias.';
