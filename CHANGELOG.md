# Changelog

Todos los cambios importantes del proyecto serán documentados en este archivo.

El formato está basado en [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [1.0.0] - 2025-12-17

### Añadido
- ✅ Arquitectura Medallón completa (Bronze, Silver, Gold)
- ✅ 23 tablas distribuidas en 3 capas
- ✅ 16 secuencias para IDs autoincrementales
- ✅ 4 funciones de transformación de datos
- ✅ 8 triggers de auditoría y actualización automática
- ✅ 5 vistas de negocio para consultas complejas
- ✅ 4 vistas materializadas con refrescado concurrente
- ✅ Sistema de auditoría completo con JSONB
- ✅ 3 roles de seguridad con permisos específicos
- ✅ Modelo dimensional Star Schema para BI
- ✅ 26 queries de ejemplo para demostración
- ✅ Documentación completa en README

### Scripts SQL
- `01_capa_bronze.sql` - Estructura de datos crudos
- `02_capa_silver.sql` - Tablas normalizadas
- `03_transformaciones_bronze_a_silver.sql` - Proceso ETL
- `04_capa_gold.sql` - Modelo dimensional
- `05_auditoria_vistas.sql` - Triggers y vistas
- `06_consulta_ejemplos.sql` - Queries de demostración

## [Unreleased]

### Por Hacer
- [ ] Implementar particionamiento por fecha
- [ ] Agregar pg_cron para automatización
- [ ] Crear API REST para vistas
- [ ] Implementar CDC (Change Data Capture)
- [ ] Dashboard en Power BI/Tableau
