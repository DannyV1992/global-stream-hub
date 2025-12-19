-- =============================================================================
-- PASO 1: PREPARACIÓN DEL ENTORNO
-- =============================================================================

-- 1.1. Crear la Base de Datos 'streaming_db'
-- En el portal de Azure PostgreSQL, crear una nueva base de datos llamada 'streaming_db' o conectarse a ella si ya existe.
CREATE DATABASE streaming_db
    WITH 
    ENCODING = 'UTF8';
-- Luego de crear la base de datos asegurarse de que 'streaming_db' sea la base de datos activa. 
COMMENT ON DATABASE streaming_db IS 'Base de datos para Global Stream Hub - Plataforma de Streaming Multimedia';

-- 1.2. Eliminación de esquemas existentes (si es necesario para limpiar) 
-- PRECAUCIÓN: Esto eliminará todos los datos existentes en estos esquemas
DROP SCHEMA IF EXISTS bronze CASCADE;
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS gold CASCADE;
DROP SCHEMA IF EXISTS audit CASCADE;

-- 1.3. Creación de los esquemas para la arquitectura Medallón
CREATE SCHEMA IF NOT EXISTS bronze;
COMMENT ON SCHEMA bronze IS 'Capa Bronze: Datos crudos sin procesar tal como llegan de las fuentes.';

CREATE SCHEMA IF NOT EXISTS silver;
COMMENT ON SCHEMA silver IS 'Capa Silver: Datos limpiados, validados y normalizados.';

CREATE SCHEMA IF NOT EXISTS gold;
COMMENT ON SCHEMA gold IS 'Capa Gold: Datos curados y agregados para BI y análisis.';

CREATE SCHEMA IF NOT EXISTS audit;
COMMENT ON SCHEMA audit IS 'Esquema de auditoría para logs de operaciones e historial de cambios.';

-- =============================================================================
-- PASO 2: CAPA BRONZE - TABLAS DE INGESTA CRUDA
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 2.1. Tabla: bronze.user_registrations
-- Almacena datos crudos de registro de usuarios tal como llegan del sistema
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.user_registrations (
    raw_id SERIAL PRIMARY KEY,
    user_id VARCHAR(100),
    nombre VARCHAR(255),
    email VARCHAR(255),
    fecha_registro VARCHAR(100), -- Se mantiene como VARCHAR para datos crudos
    pais VARCHAR(100),
    tipo_suscripcion VARCHAR(100),
    metadata_raw JSONB, -- Metadatos adicionales sin procesar
    fecha_ingesta TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE bronze.user_registrations IS 'Datos crudos de registro de usuarios sin validación ni transformación.';
COMMENT ON COLUMN bronze.user_registrations.metadata_raw IS 'Metadatos adicionales en formato JSONB como llegan de la fuente.';
COMMENT ON COLUMN bronze.user_registrations.fecha_ingesta IS 'Timestamp de cuando el registro fue ingestado en la capa Bronze.';

-- -----------------------------------------------------------------------------
-- 2.2. Tabla: bronze.raw_streaming_logs
-- Logs crudos de actividad de reproducción de contenido
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_streaming_logs (
    raw_log_id SERIAL PRIMARY KEY,
    user_id VARCHAR(100),
    content_id VARCHAR(100),
    timestamp_inicio VARCHAR(100), -- Se mantiene como VARCHAR en Bronze
    timestamp_fin VARCHAR(100),
    dispositivo VARCHAR(100),
    tiempo_visto_pct VARCHAR(50), -- Puede venir como string "85%"
    metadata_raw JSONB,
    fecha_ingesta TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE bronze.raw_streaming_logs IS 'Logs crudos de actividad de reproducción de contenido multimedia.';
COMMENT ON COLUMN bronze.raw_streaming_logs.tiempo_visto_pct IS 'Porcentaje del contenido visto, puede venir en diferentes formatos.';

-- -----------------------------------------------------------------------------
-- 2.3. Tabla: bronze.raw_catalog_data
-- Información cruda del catálogo de contenido multimedia
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_catalog_data (
    raw_catalog_id SERIAL PRIMARY KEY,
    content_id VARCHAR(100),
    titulo VARCHAR(500),
    tipo VARCHAR(100), -- 'video' o 'music'
    genero VARCHAR(500), -- Puede venir con múltiples géneros separados por comas
    director_artista VARCHAR(500), -- Puede incluir múltiples nombres
    anio VARCHAR(50),
    duracion VARCHAR(100), -- Puede venir en diferentes formatos: "120 min", "2:00:00"
    descripcion TEXT,
    metadata_raw JSONB,
    fecha_ingesta TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE bronze.raw_catalog_data IS 'Catálogo crudo de contenido multimedia sin normalización.';
COMMENT ON COLUMN bronze.raw_catalog_data.genero IS 'Géneros sin normalizar, pueden venir múltiples separados por delimitadores.';
COMMENT ON COLUMN bronze.raw_catalog_data.duracion IS 'Duración en formato crudo, requiere parsing en Silver.';

-- -----------------------------------------------------------------------------
-- 2.4. Tabla: bronze.raw_ad_impressions
-- Logs crudos de despliegue de anuncios publicitarios
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_ad_impressions (
    raw_impression_id SERIAL PRIMARY KEY,
    timestamp VARCHAR(100),
    ad_id VARCHAR(100),
    user_id VARCHAR(100),
    content_id VARCHAR(100),
    ad_placement VARCHAR(100), -- 'pre-roll', 'mid-roll', 'post-roll'
    clicked VARCHAR(50), -- Puede venir como 'true', 'false', '1', '0'
    metadata_raw JSONB,
    fecha_ingesta TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE bronze.raw_ad_impressions IS 'Logs crudos de impresiones de anuncios publicitarios.';
COMMENT ON COLUMN bronze.raw_ad_impressions.ad_placement IS 'Ubicación del anuncio en el contenido.';
COMMENT ON COLUMN bronze.raw_ad_impressions.clicked IS 'Indica si el usuario hizo clic en el anuncio, formato sin validar.';

-- -----------------------------------------------------------------------------
-- 2.5. Tabla: bronze.raw_subscription_data
-- Datos crudos de suscripciones y pagos
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_subscription_data (
    raw_subscription_id SERIAL PRIMARY KEY,
    subscription_id VARCHAR(100),
    user_id VARCHAR(100),
    plan_type VARCHAR(100), -- 'Premium', 'Con Publicidad', etc.
    start_date VARCHAR(100),
    end_date VARCHAR(100),
    payment_status VARCHAR(100), -- 'active', 'pending', 'cancelled', 'expired'
    payment_amount VARCHAR(50), -- Puede venir con símbolos de moneda
    currency VARCHAR(10),
    metadata_raw JSONB,
    fecha_ingesta TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE bronze.raw_subscription_data IS 'Datos crudos de suscripciones y estado de pagos.';
COMMENT ON COLUMN bronze.raw_subscription_data.payment_status IS 'Estado del pago sin normalizar.';
COMMENT ON COLUMN bronze.raw_subscription_data.payment_amount IS 'Monto del pago en formato crudo, puede incluir símbolos.';

-- =============================================================================
-- PASO 3: INSERCIÓN DE DATOS DE EJEMPLO EN CAPA BRONZE
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 3.1. Insertar datos de ejemplo en bronze.user_registrations
-- -----------------------------------------------------------------------------
INSERT INTO bronze.user_registrations (user_id, nombre, email, fecha_registro, pais, tipo_suscripcion, metadata_raw) VALUES
('USR001', 'María González', 'maria.gonzalez@email.com', '2024-01-15 10:30:00', 'Costa Rica', 'Premium', '{"source": "web", "referrer": "google"}'),
('USR002', 'Carlos Ramírez', 'carlos.ramirez@email.com', '2024-02-20 14:45:00', 'México', 'Con Publicidad', '{"source": "mobile_app", "referrer": "facebook"}'),
('USR003', 'Ana Rodríguez', 'ana.rodriguez@email.com', '2024-03-10 09:15:00', 'España', 'Premium', '{"source": "web", "referrer": "direct"}'),
('USR004', 'Luis Fernández', 'luis.fernandez@email.com', '2024-04-05 16:20:00', 'Argentina', 'Con Publicidad', '{"source": "mobile_app", "referrer": "instagram"}'),
('USR005', 'Sofía Martínez', 'sofia.martinez@email.com', '2024-05-12 11:00:00', 'Colombia', 'Premium', '{"source": "web", "referrer": "youtube"}'),
('USR006', 'Diego Torres', 'diego.torres@email.com', '2024-06-18 13:30:00', 'Chile', 'Con Publicidad', '{"source": "mobile_app", "referrer": "twitter"}'),
('USR007', 'Laura Sánchez', 'laura.sanchez@email.com', '2024-07-22 15:45:00', 'Perú', 'Premium', '{"source": "web", "referrer": "google"}'),
('USR008', 'Pedro Morales', 'pedro.morales@email.com', '2024-08-30 08:20:00', 'Ecuador', 'Con Publicidad', '{"source": "tablet", "referrer": "email_campaign"}'),
('USR009', 'Miguel Herrera', 'miguel.herrera@email.com', '2024-09-10 09:00:00', 'México', 'Premium', '{"source": "web", "referrer": "linkedin"}'),
('USR010', 'Valentina López', 'valentina.lopez@email.com', '2024-09-12 18:30:00', 'España', 'Con Publicidad', '{"source": "smart_tv", "referrer": "promo_banner"}'),
('USR011', 'Jorge Castillo', 'jorge.castillo@email.com', '2024-09-15 21:10:00', 'Chile', 'Premium', '{"source": "mobile_app", "referrer": "friend_referral"}'),
('USR012', 'Camila Rojas', 'camila.rojas@email.com', '2024-09-18 11:45:00', 'Colombia', 'Con Publicidad', '{"source": "tablet", "referrer": "email_campaign"}');

-- -----------------------------------------------------------------------------
-- 3.2. Insertar datos de ejemplo en bronze.raw_catalog_data
-- -----------------------------------------------------------------------------
INSERT INTO bronze.raw_catalog_data (content_id, titulo, tipo, genero, director_artista, anio, duracion, descripcion, metadata_raw) VALUES
('CNT001', 'Aventuras en el Espacio', 'video', 'Ciencia Ficción, Acción', 'Steven Cosmos', '2023', '120 min', 'Una épica aventura espacial.', '{"rating": "PG-13", "language": "español"}'),
('CNT002', 'Ritmos del Caribe', 'music', 'Reggaeton, Pop Latino', 'Los Tropicales', '2024', '3:45', 'Álbum de éxitos latinos.', '{"tracks": 12, "label": "Universal"}'),
('CNT003', 'Misterio en París', 'video', 'Thriller, Drama', 'Marie Dubois', '2022', '1:45:00', 'Un thriller psicológico intenso.', '{"rating": "R", "language": "francés"}'),
('CNT004', 'Sinfonía Nocturna', 'music', 'Clásica, Instrumental', 'Orquesta Sinfónica Nacional', '2021', '45:30', 'Composiciones clásicas modernas.', '{"tracks": 8, "label": "Sony Classical"}'),
('CNT005', 'Comedia Familiar', 'video', 'Comedia', 'Roberto Risa', '2024', '95 min', 'Risas para toda la familia.', '{"rating": "PG", "language": "español"}'),
('CNT006', 'Beats Electrónicos', 'music', 'Electrónica, Dance', 'DJ Pulsar', '2024', '50:00', 'Los mejores beats para bailar.', '{"tracks": 15, "label": "Independent"}'),
('CNT007', 'Documental Naturaleza', 'video', 'Documental', 'National Geo Team', '2023', '2:10:00', 'La vida salvaje en HD.', '{"rating": "G", "language": "inglés"}'),
('CNT008', 'Jazz en Vivo', 'music', 'Jazz', 'Quinteto Blue Note', '2023', '1:15:00', 'Concierto grabado en Nueva York.', '{"tracks": 10, "label": "Blue Note Records"}'),
('CNT009', 'Crónicas del Futuro', 'video', 'Ciencia Ficción, Drama', 'Lucía Martínez', '2023', '130 min', 'Serie futurista sobre inteligencia artificial.', '{"rating": "PG-13", "language": "español"}'),
('CNT010', 'Rock Clásico Vol. 1', 'music', 'Rock, Clásicos', 'The Legends', '2020', '58:30', 'Selección de rock clásico remasterizado.', '{"tracks": 14, "label": "Warner"}'),
('CNT011', 'Cocina con Sabor', 'video', 'Reality, Cocina', 'Canal Gourmet', '2024', '50:00', 'Programa de cocina con chefs invitados.', '{"rating": "G", "language": "español"}'),
('CNT012', 'Meditación Guiada', 'music', 'Ambient, Relajación', 'Luz Interior', '2022', '40:00', 'Música para meditar y relajarse.', '{"tracks": 9, "label": "Mindful Sounds"}'),
('CNT013', 'Historia de la Tecnología', 'video', 'Documental, Educación', 'TechDoc Studio', '2021', '1:30:00', 'Documental sobre la evolución de la tecnología.', '{"rating": "PG", "language": "inglés"}'),
('CNT014', 'Fiesta Latina', 'music', 'Pop Latino, Dance', 'DJ Caribe', '2024', '1:05:00', 'Mezcla continua de éxitos latinos.', '{"tracks": 18, "label": "Independent"}'),
('CNT015', 'Terror en la Noche', 'video', 'Terror, Suspenso', 'Horror Films Inc.', '2023', '1:40:00', 'Película de terror psicológico.', '{"rating": "R", "language": "español"}');

-- -----------------------------------------------------------------------------
-- 3.3. Insertar datos de ejemplo en bronze.raw_streaming_logs
-- -----------------------------------------------------------------------------
INSERT INTO bronze.raw_streaming_logs (user_id, content_id, timestamp_inicio, timestamp_fin, dispositivo, tiempo_visto_pct, metadata_raw) VALUES
('USR001', 'CNT001', '2024-09-01 20:00:00', '2024-09-01 22:00:00', 'Smart TV', '100%', '{"quality": "4K", "bandwidth_mbps": 25}'),
('USR001', 'CNT002', '2024-09-02 10:30:00', '2024-09-02 10:33:45', 'Mobile', '100%', '{"quality": "HD", "bandwidth_mbps": 10}'),
('USR002', 'CNT001', '2024-09-01 21:00:00', '2024-09-01 21:45:00', 'Tablet', '37.5%', '{"quality": "HD", "bandwidth_mbps": 15}'),
('USR003', 'CNT003', '2024-09-03 19:00:00', '2024-09-03 20:45:00', 'Web Browser', '100%', '{"quality": "4K", "bandwidth_mbps": 30}'),
('USR004', 'CNT005', '2024-09-04 16:00:00', '2024-09-04 17:35:00', 'Mobile', '100%', '{"quality": "SD", "bandwidth_mbps": 5}'),
('USR005', 'CNT007', '2024-09-05 14:00:00', '2024-09-05 15:10:00', 'Smart TV', '53.8%', '{"quality": "4K", "bandwidth_mbps": 25}'),
('USR006', 'CNT006', '2024-09-06 22:00:00', '2024-09-06 22:50:00', 'Web Browser', '100%', '{"quality": "HD", "bandwidth_mbps": 20}'),
('USR007', 'CNT004', '2024-09-07 08:00:00', '2024-09-07 08:45:30', 'Mobile', '100%', '{"quality": "HD", "bandwidth_mbps": 12}'),
('USR008', 'CNT002', '2024-09-08 12:00:00', '2024-09-08 12:03:45', 'Tablet', '100%', '{"quality": "HD", "bandwidth_mbps": 15}'),
('USR001', 'CNT003', '2024-09-09 21:00:00', '2024-09-09 22:40:00', 'Web Browser', '95%', '{"quality": "HD", "bandwidth_mbps": 18}'),
('USR001', 'CNT004', '2024-09-10 07:30:00', '2024-09-10 08:10:00', 'Mobile', '100%', '{"quality": "SD", "bandwidth_mbps": 6}'),
('USR002', 'CNT005', '2024-09-11 19:00:00', '2024-09-11 20:25:00', 'Smart TV', '100%', '{"quality": "HD", "bandwidth_mbps": 20}'),
('USR002', 'CNT002', '2024-09-12 09:15:00', '2024-09-12 09:18:40', 'Tablet', '100%', '{"quality": "HD", "bandwidth_mbps": 12}'),
('USR003', 'CNT007', '2024-09-12 21:00:00', '2024-09-12 22:50:00', 'Smart TV', '80%', '{"quality": "4K", "bandwidth_mbps": 25}'),
('USR003', 'CNT008', '2024-09-13 10:00:00', '2024-09-13 11:10:00', 'Web Browser', '100%', '{"quality": "HD", "bandwidth_mbps": 15}'),
('USR004', 'CNT001', '2024-09-13 16:30:00', '2024-09-13 17:10:00', 'Mobile', '60%', '{"quality": "SD", "bandwidth_mbps": 5}'),
('USR004', 'CNT006', '2024-09-14 22:00:00', '2024-09-14 22:40:00', 'Smart TV', '90%', '{"quality": "HD", "bandwidth_mbps": 18}'),
('USR005', 'CNT004', '2024-09-14 08:00:00', '2024-09-14 08:35:00', 'Tablet', '100%', '{"quality": "HD", "bandwidth_mbps": 10}'),
('USR005', 'CNT002', '2024-09-15 12:00:00', '2024-09-15 12:03:30', 'Mobile', '100%', '{"quality": "HD", "bandwidth_mbps": 9}'),
('USR006', 'CNT003', '2024-09-15 21:00:00', '2024-09-15 22:45:00', 'Web Browser', '100%', '{"quality": "4K", "bandwidth_mbps": 28}'),
('USR006', 'CNT007', '2024-09-16 19:30:00', '2024-09-16 20:20:00', 'Smart TV', '70%', '{"quality": "4K", "bandwidth_mbps": 22}'),
('USR007', 'CNT005', '2024-09-16 17:00:00', '2024-09-16 18:25:00', 'Mobile', '100%', '{"quality": "HD", "bandwidth_mbps": 11}'),
('USR007', 'CNT006', '2024-09-17 22:10:00', '2024-09-17 22:55:00', 'Web Browser', '100%', '{"quality": "HD", "bandwidth_mbps": 16}'),
('USR008', 'CNT001', '2024-09-17 15:00:00', '2024-09-17 16:30:00', 'Smart TV', '75%', '{"quality": "HD", "bandwidth_mbps": 19}'),
('USR008', 'CNT008', '2024-09-18 09:00:00', '2024-09-18 10:12:00', 'Tablet', '100%', '{"quality": "HD", "bandwidth_mbps": 13}'),
('USR009', 'CNT009', '2024-09-19 20:00:00', '2024-09-19 22:10:00', 'Smart TV', '100%', '{"quality": "4K", "bandwidth_mbps": 26}'),
('USR009', 'CNT010', '2024-09-20 09:30:00', '2024-09-20 10:25:00', 'Mobile', '100%', '{"quality": "HD", "bandwidth_mbps": 8}'),
('USR009', 'CNT002', '2024-09-21 13:00:00', '2024-09-21 13:03:40', 'Tablet', '100%', '{"quality": "HD", "bandwidth_mbps": 10}'),
('USR010', 'CNT011', '2024-09-19 18:00:00', '2024-09-19 18:50:00', 'Web Browser', '100%', '{"quality": "HD", "bandwidth_mbps": 14}'),
('USR010', 'CNT014', '2024-09-20 22:15:00', '2024-09-20 23:20:00', 'Smart TV', '100%', '{"quality": "HD", "bandwidth_mbps": 18}'),
('USR010', 'CNT003', '2024-09-21 21:10:00', '2024-09-21 22:40:00', 'Mobile', '90%', '{"quality": "HD", "bandwidth_mbps": 12}'),
('USR011', 'CNT012', '2024-09-18 23:00:00', '2024-09-18 23:40:00', 'Mobile', '100%', '{"quality": "SD", "bandwidth_mbps": 4}'),
('USR011', 'CNT013', '2024-09-19 19:00:00', '2024-09-19 20:30:00', 'Web Browser', '100%', '{"quality": "HD", "bandwidth_mbps": 17}'),
('USR011', 'CNT001', '2024-09-20 20:00:00', '2024-09-20 21:10:00', 'Smart TV', '60%', '{"quality": "4K", "bandwidth_mbps": 24}'),
('USR012', 'CNT014', '2024-09-18 18:30:00', '2024-09-18 19:40:00', 'Smart TV', '100%', '{"quality": "HD", "bandwidth_mbps": 21}'),
('USR012', 'CNT015', '2024-09-19 22:45:00', '2024-09-19 23:10:00', 'Web Browser', '85%', '{"quality": "HD", "bandwidth_mbps": 15}'),
('USR012', 'CNT005', '2024-09-20 16:00:00', '2024-09-20 17:30:00', 'Mobile', '100%', '{"quality": "HD", "bandwidth_mbps": 9}'),
('USR001', 'CNT002', '2024-09-22 10:00:00', '2024-09-22 10:01:30', 'Mobile', '40%', '{"quality": "SD", "bandwidth_mbps": 3}'),
('USR003', 'CNT006', '2024-09-22 22:00:00', '2024-09-22 22:20:00', 'Web Browser', '50%', '{"quality": "HD", "bandwidth_mbps": 10}'),
('USR005', 'CNT009', '2024-09-23 19:15:00', '2024-09-23 21:10:00', 'Smart TV', '95%', '{"quality": "4K", "bandwidth_mbps": 27}'),
('USR006', 'CNT010', '2024-09-23 08:00:00', '2024-09-23 08:52:00', 'Mobile', '100%', '{"quality": "HD", "bandwidth_mbps": 11}'),
('USR007', 'CNT011', '2024-09-24 17:30:00', '2024-09-24 18:15:00', 'Tablet', '90%', '{"quality": "HD", "bandwidth_mbps": 13}'),
('USR008', 'CNT012', '2024-09-24 23:00:00', '2024-09-24 23:35:00', 'Mobile', '100%', '{"quality": "SD", "bandwidth_mbps": 5}'),
('USR009', 'CNT015', '2024-09-25 21:30:00', '2024-09-25 23:05:00', 'Smart TV', '100%', '{"quality": "HD", "bandwidth_mbps": 20}'),
('USR010', 'CNT007', '2024-09-25 20:00:00', '2024-09-25 21:55:00', 'Smart TV', '85%', '{"quality": "4K", "bandwidth_mbps": 23}'),
('USR011', 'CNT004', '2024-09-26 07:10:00', '2024-09-26 07:45:00', 'Mobile', '100%', '{"quality": "HD", "bandwidth_mbps": 8}'),
('USR012', 'CNT003', '2024-09-26 22:00:00', '2024-09-26 23:35:00', 'Web Browser', '100%', '{"quality": "HD", "bandwidth_mbps": 19}');

-- -----------------------------------------------------------------------------
-- 3.4. Insertar datos de ejemplo en bronze.raw_subscription_data
-- -----------------------------------------------------------------------------
INSERT INTO bronze.raw_subscription_data (subscription_id, user_id, plan_type, start_date, end_date, payment_status, payment_amount, currency, metadata_raw) VALUES
('SUB001', 'USR001', 'Premium', '2024-01-15', '2025-01-15', 'active', '$14.99', 'USD', '{"payment_method": "credit_card", "auto_renew": true}'),
('SUB002', 'USR002', 'Con Publicidad', '2024-02-20', '2025-02-20', 'active', '$4.99', 'USD', '{"payment_method": "paypal", "auto_renew": true}'),
('SUB003', 'USR003', 'Premium', '2024-03-10', '2025-03-10', 'active', '12.99', 'EUR', '{"payment_method": "credit_card", "auto_renew": true}'),
('SUB004', 'USR004', 'Con Publicidad', '2024-04-05', '2024-10-05', 'expired', '$4.99', 'USD', '{"payment_method": "debit_card", "auto_renew": false}'),
('SUB005', 'USR005', 'Premium', '2024-05-12', '2025-05-12', 'active', '$14.99', 'USD', '{"payment_method": "credit_card", "auto_renew": true}'),
('SUB006', 'USR006', 'Con Publicidad', '2024-06-18', '2025-06-18', 'active', '$4.99', 'USD', '{"payment_method": "paypal", "auto_renew": true}'),
('SUB007', 'USR007', 'Premium', '2024-07-22', '2025-07-22', 'pending', '$14.99', 'USD', '{"payment_method": "credit_card", "auto_renew": true}'),
('SUB008', 'USR008', 'Con Publicidad', '2024-08-30', '2025-08-30', 'active', '$4.99', 'USD', '{"payment_method": "google_pay", "auto_renew": true}'),
('SUB009', 'USR009', 'Premium', '2024-09-10', '2025-09-10', 'active',  '$14.99', 'USD', '{"payment_method": "credit_card", "auto_renew": true}'),
('SUB010', 'USR010', 'Con Publicidad', '2024-09-12', '2025-09-12', 'active', '$4.99', 'USD', '{"payment_method": "debit_card", "auto_renew": true}'),
('SUB011', 'USR011', 'Premium', '2024-09-15', '2025-09-15', 'active', '$14.99', 'USD', '{"payment_method": "paypal", "auto_renew": true}'),
('SUB012', 'USR012', 'Con Publicidad', '2024-09-18', '2025-09-18', 'active', '$4.99', 'USD', '{"payment_method": "google_pay", "auto_renew": true}');

-- -----------------------------------------------------------------------------
-- 3.5. Insertar datos de ejemplo en bronze.raw_ad_impressions
-- -----------------------------------------------------------------------------
INSERT INTO bronze.raw_ad_impressions (timestamp, ad_id, user_id, content_id, ad_placement, clicked, metadata_raw) VALUES
('2024-09-01 21:00:00', 'AD001', 'USR002', 'CNT001', 'pre-roll', 'false', '{"ad_duration_sec": 30, "advertiser": "Coca-Cola"}'),
('2024-09-01 21:30:00', 'AD002', 'USR002', 'CNT001', 'mid-roll', 'true', '{"ad_duration_sec": 15, "advertiser": "Nike"}'),
('2024-09-04 16:00:00', 'AD001', 'USR004', 'CNT005', 'pre-roll', 'false', '{"ad_duration_sec": 30, "advertiser": "Coca-Cola"}'),
('2024-09-04 17:00:00', 'AD003', 'USR004', 'CNT005', 'mid-roll', 'false', '{"ad_duration_sec": 20, "advertiser": "Samsung"}'),
('2024-09-06 22:00:00', 'AD002', 'USR006', 'CNT006', 'pre-roll', 'true', '{"ad_duration_sec": 15, "advertiser": "Nike"}'),
('2024-09-06 22:25:00', 'AD004', 'USR006', 'CNT006', 'mid-roll', 'false', '{"ad_duration_sec": 25, "advertiser": "Toyota"}'),
('2024-09-08 12:00:00', 'AD001', 'USR008', 'CNT002', 'pre-roll', 'false', '{"ad_duration_sec": 30, "advertiser": "Coca-Cola"}'),
('2024-09-11 19:00:00', 'AD001', 'USR002', 'CNT005', 'pre-roll', 'false', '{"ad_duration_sec": 30, "advertiser": "Coca-Cola"}'),
('2024-09-11 19:50:00', 'AD003', 'USR002', 'CNT005', 'mid-roll', 'false', '{"ad_duration_sec": 20, "advertiser": "Samsung"}'),
('2024-09-12 09:15:00', 'AD002', 'USR002', 'CNT002', 'pre-roll', 'true', '{"ad_duration_sec": 15, "advertiser": "Nike"}'),
('2024-09-13 16:30:00', 'AD004', 'USR004', 'CNT001', 'pre-roll', 'false', '{"ad_duration_sec": 25, "advertiser": "Toyota"}'),
('2024-09-13 17:00:00', 'AD001', 'USR004', 'CNT001', 'mid-roll', 'false', '{"ad_duration_sec": 30, "advertiser": "Coca-Cola"}'),
('2024-09-14 22:00:00', 'AD002', 'USR004', 'CNT006', 'pre-roll', 'true', '{"ad_duration_sec": 15, "advertiser": "Nike"}'),
('2024-09-14 22:30:00', 'AD003', 'USR004', 'CNT006', 'mid-roll', 'false', '{"ad_duration_sec": 20, "advertiser": "Samsung"}'),
('2024-09-15 21:00:00', 'AD001', 'USR006', 'CNT003', 'pre-roll', 'false', '{"ad_duration_sec": 30, "advertiser": "Coca-Cola"}'),
('2024-09-15 22:00:00', 'AD004', 'USR006', 'CNT003', 'mid-roll', 'false', '{"ad_duration_sec": 25, "advertiser": "Toyota"}'),
('2024-09-16 19:30:00', 'AD002', 'USR006', 'CNT007', 'pre-roll', 'true', '{"ad_duration_sec": 15, "advertiser": "Nike"}'),
('2024-09-16 20:00:00', 'AD003', 'USR006', 'CNT007', 'mid-roll', 'false', '{"ad_duration_sec": 20, "advertiser": "Samsung"}'),
('2024-09-17 15:00:00', 'AD001', 'USR008', 'CNT001', 'pre-roll', 'false', '{"ad_duration_sec": 30, "advertiser": "Coca-Cola"}'),
('2024-09-17 16:00:00', 'AD004', 'USR008', 'CNT001', 'mid-roll', 'false', '{"ad_duration_sec": 25, "advertiser": "Toyota"}'),
('2024-09-18 09:00:00', 'AD002', 'USR008', 'CNT008', 'pre-roll', 'true', '{"ad_duration_sec": 15, "advertiser": "Nike"}'),
('2024-09-19 18:00:00', 'AD003', 'USR010', 'CNT011', 'pre-roll', 'false', '{"ad_duration_sec": 20, "advertiser": "Samsung"}'),
('2024-09-20 22:15:00', 'AD001', 'USR010', 'CNT014', 'pre-roll', 'false', '{"ad_duration_sec": 30, "advertiser": "Coca-Cola"}'),
('2024-09-20 23:00:00', 'AD002', 'USR010', 'CNT014', 'mid-roll', 'true', '{"ad_duration_sec": 15, "advertiser": "Nike"}'),
('2024-09-21 21:10:00', 'AD004', 'USR010', 'CNT003', 'pre-roll', 'false', '{"ad_duration_sec": 25, "advertiser": "Toyota"}'),
('2024-09-21 22:00:00', 'AD003', 'USR010', 'CNT003', 'mid-roll', 'false', '{"ad_duration_sec": 20, "advertiser": "Samsung"}'),
('2024-09-18 18:30:00', 'AD001', 'USR012', 'CNT014', 'pre-roll', 'false', '{"ad_duration_sec": 30, "advertiser": "Coca-Cola"}'),
('2024-09-18 19:30:00', 'AD002', 'USR012', 'CNT014', 'mid-roll', 'true', '{"ad_duration_sec": 15, "advertiser": "Nike"}'),
('2024-09-19 22:45:00', 'AD004', 'USR012', 'CNT015', 'pre-roll', 'false', '{"ad_duration_sec": 25, "advertiser": "Toyota"}'),
('2024-09-19 23:30:00', 'AD003', 'USR012', 'CNT015', 'mid-roll', 'false', '{"ad_duration_sec": 20, "advertiser": "Samsung"}'),
('2024-09-20 16:00:00', 'AD001', 'USR012', 'CNT005', 'pre-roll', 'false', '{"ad_duration_sec": 30, "advertiser": "Coca-Cola"}'),
('2024-09-20 17:00:00', 'AD002', 'USR012', 'CNT005', 'mid-roll', 'true', '{"ad_duration_sec": 15, "advertiser": "Nike"}');
