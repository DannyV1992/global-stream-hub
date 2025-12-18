# 🎬 Global Stream Hub - Plataforma de Streaming Multimedia

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17-336791?style=flat&logo=postgresql)](https://www.postgresql.org/)
[![Arquitectura](https://img.shields.io/badge/Arquitectura-Medallón-blue)](https://www.databricks.com/glossary/medallion-architecture)
[![Licencia](https://img.shields.io/badge/Licencia-MIT-green)](LICENSE)

> **Proyecto final** - Curso de arquitectura de bases de datos  
> Sistema de base de datos escalable implementando arquitectura Medallón para una plataforma global de streaming de video y música.

---

## 📋 Tabla de contenidos

- [Descripción](#-descripción)
- [Arquitectura](#-arquitectura)
- [Características](#-características)
- [Requisitos](#-requisitos)
- [Instalación](#-instalación)
- [Estructura del Proyecto](#-estructura-del-proyecto)
- [Uso](#-uso)
- [Modelo de Datos](#-modelo-de-datos)
- [Consultas de Ejemplo](#-consultas-de-ejemplo)
- [Auditoría y Seguridad](#-auditoría-y-seguridad)
- [Contribuciones](#-contribuciones)
- [Licencia](#-licencia)

---

## 📖 Descripción

**Global Stream Hub** es una arquitectura de base de datos robusta y escalable diseñada para soportar una plataforma global de streaming multimedia. El sistema maneja:

- 🎥 **Contenido de video y música**
- 👥 **Gestión de usuarios y suscripciones**
- 📊 **Análisis de actividad y engagement**
- 📺 **Sistema de publicidad con métricas de CTR**
- 🎵 **Playlists personalizadas**
- 🔍 **Auditoría completa de cambios**

### Modelo de negocio

La plataforma opera bajo un modelo híbrido:
- **Suscripción Premium**: Acceso sin publicidad y contenido exclusivo ($14.99/mes)
- **Plan con Publicidad**: Acceso con anuncios intercalados ($4.99/mes)

---

## 🏗️ Arquitectura

### Arquitectura medallón (Bronze → Silver → Gold)

```
┌─────────────────────────────────────────────────────────────┐
│                     CAPA GOLD (Curated)                     │
│  ┌──────────────────────┐    ┌───────────────────────────┐  │
│  │   Dimensiones (6)    │    │   Hechos (3)              │  │
│  │  • dim_users         │    │  • fact_user_activity     │  │
│  │  • dim_content       │    │  • fact_content_popularity│  │
│  │  • dim_devices       │    │  • fact_ad_performance    │  │
│  │  • dim_genres        │    └───────────────────────────┘  │
│  │  • dim_date          │    ┌───────────────────────────┐  │
│  │  • dim_time_of_day   │    │  Vistas (5) + MVs (4)     │  │
│  └──────────────────────┘    └───────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │ Transformación
                              │
┌─────────────────────────────────────────────────────────────┐
│                  CAPA SILVER (Normalized)                   │
│  ┌──────────────┬──────────────┬──────────────┬──────────┐  │
│  │   Users      │   Content    │  Streaming   │   Ads    │  │
│  │ Subscriptions│   Genres     │   Sessions   │ Campaigns│  │
│  │   Countries  │   Artists    │   Devices    │Impressions│ │
│  └──────────────┴──────────────┴──────────────┴──────────┘  │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │ ETL & Cleaning
                              │
┌─────────────────────────────────────────────────────────────┐
│                   CAPA BRONZE (Raw Data)                    │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  • user_registrations      • raw_streaming_logs        │ │
│  │  • raw_catalog_data        • raw_ad_impressions        │ │
│  │  • raw_subscription_data                               │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

---

## ✨ Características

### 🎯 Funcionalidades clave

- ✅ **Arquitectura medallón completa** (Bronze, Silver, Gold)
- ✅ **23 tablas** distribuidas en capas normalizadas
- ✅ **16 secuencias** para IDs autoincrementales
- ✅ **4 funciones de transformación** (parsing de datos)
- ✅ **8 triggers** de auditoría y actualización automática
- ✅ **5 vistas de negocio** para consultas complejas
- ✅ **4 vistas materializadas** con refrescado concurrente
- ✅ **Sistema de auditoría** completo con JSONB
- ✅ **3 roles de seguridad** (ETL, BI Analyst, API Consumer)
- ✅ **Modelo dimensional** Star Schema para BI

### 📊 Métricas y KPIs

El sistema proporciona análisis en tiempo real de:
- Usuarios activos diarios (DAU)
- Contenido más popular por género/país
- Tasa de completado de contenido
- CTR (Click-Through Rate) de publicidad
- Horas de streaming por usuario
- Tendencias temporales (diarias, semanales, mensuales)

---

## 🔧 Requisitos

### Software necesario

- **PostgreSQL 17** o superior
- **DBeaver** (recomendado) o cualquier cliente PostgreSQL
- **Azure PostgreSQL** (opcional, para producción)

### Conocimientos requeridos

- SQL avanzado
- Conceptos de data warehousing
- Arquitectura Medallón
- Modelo dimensional (Star Schema)

---

## 🚀 Instalación

### 1. Crear base de datos

```sql
-- En tu servidor PostgreSQL
CREATE DATABASE streaming_db;
```

### 2. Ejecutar scripts en orden

Ejecuta los scripts SQL en el siguiente orden obligatorio:

```bash
# 1. Crear estructura Bronze
psql -d streaming_db -f 01_capa_bronze.sql

# 2. Crear estructura Silver
psql -d streaming_db -f 02_capa_silver.sql

# 3. Transformar datos Bronze → Silver
psql -d streaming_db -f 03_transformaciones_bronze_a_silver.sql

# 4. Crear capa Gold (dimensional)
psql -d streaming_db -f 04_capa_gold.sql

# 5. Configurar auditoría y vistas
psql -d streaming_db -f 05_auditoria_vistas.sql
```

### 3. Verificar instalación

```sql
-- Verificar que todos los esquemas existan
SELECT schema_name FROM information_schema.schemata 
WHERE schema_name IN ('bronze', 'silver', 'gold', 'audit');

-- Ver resumen de objetos creados
SELECT 'Tablas Bronze' AS tipo, COUNT(*) FROM information_schema.tables 
    WHERE table_schema = 'bronze' AND table_type = 'BASE TABLE'
UNION ALL
SELECT 'Tablas Silver', COUNT(*) FROM information_schema.tables 
    WHERE table_schema = 'silver' AND table_type = 'BASE TABLE'
UNION ALL
SELECT 'Tablas Gold', COUNT(*) FROM information_schema.tables 
    WHERE table_schema = 'gold' AND table_type = 'BASE TABLE';
```

---

## 📁 Estructura del proyecto

```
global-stream-hub/
│
├── 01_capa_bronze.sql                      # Estructura de datos crudos
├── 02_capa_silver.sql                      # Tablas normalizadas
├── 03_transformaciones_bronze_a_silver.sql # ETL Bronze → Silver
├── 04_capa_gold.sql                        # Modelo dimensional
├── 05_auditoria_vistas.sql                 # Triggers, vistas y MVs
├── 06_consulta_ejemplos.sql                # Queries de demostración
│
├── docs/
│   ├── diagrams/
│   │   ├── bronze_erd.png
│   │   ├── silver_erd.png
│   │   └── gold_erd.png
│   └── design_document.pdf
│
├── README.md                               # Este archivo
└── LICENSE
```

---

## 💾 Modelo de datos

### Capa Bronze (5 tablas)

Datos crudos sin procesar:

- `user_registrations` - Registros de usuarios
- `raw_streaming_logs` - Logs de reproducción
- `raw_catalog_data` - Catálogo multimedia
- `raw_ad_impressions` - Impresiones de anuncios
- `raw_subscription_data` - Datos de suscripciones

### Capa Silver (16 tablas)

Datos normalizados y limpios:

**Usuarios y suscripciones:**
- `users`, `countries`, `subscription_plans`, `subscriptions`

**Contenido multimedia:**
- `content`, `genres`, `content_genres`
- `artists_directors`, `content_artists`

**Actividad:**
- `streaming_sessions`, `devices`
- `playlists`, `playlist_items`

**Publicidad:**
- `ad_campaigns`, `ads`, `ad_impressions`

### Capa Gold (9 tablas + 5 vistas + 4 MVs)

**Dimensiones (6):**
- `dim_users`, `dim_content`, `dim_devices`
- `dim_genres`, `dim_date`, `dim_time_of_day`

**Hechos (3):**
- `fact_user_activity` - Actividad de usuarios
- `fact_content_popularity` - Popularidad de contenido
- `fact_ad_performance` - Rendimiento de anuncios

**Vistas de negocio (5):**
- `vw_user_profile_details`
- `vw_content_catalog_summary`
- `vw_user_playback_history`
- `vw_playlist_details`
- `vw_ad_performance_summary`

**Vistas materializadas (4):**
- `mv_monthly_user_activity`
- `mv_content_popularity_by_genre`
- `mv_ad_performance_metrics`
- `mv_daily_platform_metrics`

---

## 🔍 Consultas de ejemplo

### Top 10 contenidos más populares

```sql
SELECT 
    title,
    content_type,
    genres,
    total_views,
    unique_viewers,
    avg_completion_rate
FROM gold.vw_content_catalog_summary
WHERE total_views > 0
ORDER BY total_views DESC
LIMIT 10;
```

### KPIs diarios de la plataforma

```sql
SELECT 
    metric_date,
    daily_active_users,
    total_sessions,
    ROUND(total_hours_streamed, 2) AS horas_streaming,
    total_ad_impressions,
    total_ad_clicks
FROM gold.mv_daily_platform_metrics
WHERE metric_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY metric_date DESC;
```

### Usuarios más activos del mes

```sql
SELECT 
    full_name,
    email,
    country_name,
    plan_name,
    total_sessions,
    ROUND(total_hours_watched, 2) AS horas_vistas
FROM gold.mv_monthly_user_activity
WHERE activity_month = DATE_TRUNC('month', CURRENT_DATE)::DATE
ORDER BY total_hours_watched DESC
LIMIT 10;
```

📄 **Más ejemplos** disponibles en: [`06_consulta_ejemplos.sql`](06_consulta_ejemplos.sql)

---

## 🔐 Auditoría y seguridad

### Sistema de auditoría

El sistema captura automáticamente:

- ✅ Todos los cambios en usuarios (`audit.history_users`)
- ✅ Modificaciones de suscripciones (`audit.history_subscriptions`)
- ✅ Actualizaciones de contenido (`audit.history_content`)
- ✅ Almacenamiento en formato JSONB (OLD y NEW data)

```sql
-- Ver últimos cambios en usuarios
SELECT 
    user_id,
    operation_type,
    changed_by,
    changed_at,
    new_data->>'full_name' AS usuario_modificado
FROM audit.history_users
ORDER BY changed_at DESC
LIMIT 10;
```

### Roles y permisos

El sistema implementa el principio de **mínimo privilegio**:

| Rol | Acceso | Descripción |
|-----|--------|-------------|
| `etl_process_role` | Lectura Bronze, Escritura Silver | Procesos ETL |
| `bi_analyst_role` | Solo lectura Gold | Analistas de BI |
| `api_consumer_role` | Solo vistas Gold | APIs externas |

---

## 🛠️ Mantenimiento

### Refrescar vistas materializadas

```sql
-- Refrescar todas las MVs de forma concurrente
SELECT gold.refresh_all_materialized_views();
```

### Limpiar datos de prueba

```sql
-- Eliminar datos de ejemplo (usar con precaución)
TRUNCATE bronze.user_registrations CASCADE;
TRUNCATE bronze.raw_streaming_logs CASCADE;
-- ... (repetir para otras tablas Bronze)
```

---

## 📈 Roadmap

### Próximas mejoras

- [ ] Implementar particionamiento en `streaming_sessions` por fecha
- [ ] Agregar soporte para pg_cron (automatización de ETL)
- [ ] Crear API REST para exposición de vistas
- [ ] Implementar CDC (Change Data Capture)
- [ ] Añadir más métricas de engagement
- [ ] Dashboard en Power BI / Tableau

---

## 👥 Autores

**Daniel Vásquez**  
Curso de Arquitectura de Bases de Datos  
Diciembre 2025

---

## 🤝 Contribuciones

Las contribuciones son bienvenidas. Por favor:

1. Fork el proyecto
2. Crea una rama para tu feature (`git checkout -b feature/New_Feature`)
3. Commit tus cambios (`git commit -m 'Add some new feature'`)
4. Push a la rama (`git push origin feature/New_Feature`)
5. Abre un Pull Request

---

## 📝 Licencia

Este proyecto está bajo la Licencia MIT. Ver el archivo `LICENSE` para más detalles.

---

## 📞 Contacto

¿Preguntas o sugerencias?

- 📧 Email: dava01cr@gmail.com
- 💼 LinkedIn: danielvasquezcr
- 🐙 GitHub: [@DannyV1992](https://github.com/DannyV1992)

---

## 🙏 Agradecimientos

- **Profesor Leonardo Fabio Fernández** - Curso de Arquitectura de Bases de Datos
- **PostgreSQL Community** - Por la excelente documentación
- **Databricks** - Por el concepto de arquitectura Medallón

---

<div align="center">

**⭐ Si este proyecto te fue útil, considera darle una estrella ⭐**

[![GitHub stars](https://img.shields.io/github/stars/DannyV1992/global-stream-hub?style=social)](https://github.com/DannyV1992/global-stream-hub/stargazers)
