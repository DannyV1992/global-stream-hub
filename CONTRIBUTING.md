# Guía de contribución

¡Gracias por tu interés en contribuir a Global Stream Hub!

## 🤝 Cómo contribuir

### 1. Fork y Clone

```bash
# Fork el repositorio en GitHub
# Luego clona tu fork
git clone https://github.com/DannyV1992/global-stream-hub.git
cd global-stream-hub
```

### 2. Crear una rama

```bash
git checkout -b feature/nueva-funcionalidad
```

### 3. Hacer cambios

- Sigue las convenciones de código SQL existentes
- Comenta tus queries apropiadamente
- Asegúrate de que los scripts sean idempotentes (pueden ejecutarse múltiples veces)

### 4. Commit

```bash
git add .
git commit -m "feat: descripción clara del cambio"
```

### 5. Push y Pull Request

```bash
git push origin feature/nueva-funcionalidad
```

Luego abre un Pull Request en GitHub.

## 📝 Convenciones de código

### SQL
- Usa UPPERCASE para palabras clave SQL (`SELECT`, `FROM`, `WHERE`)
- Usa snake_case para nombres de tablas y columnas
- Indenta con 4 espacios
- Comenta secciones complejas

### Commits
- `feat:` - Nueva funcionalidad
- `fix:` - Corrección de bugs
- `docs:` - Cambios en documentación
- `refactor:` - Refactorización de código
- `perf:` - Mejoras de rendimiento

## ✅ Checklist antes de PR

- [ ] El código SQL se ejecuta sin errores
- [ ] Se agregaron comentarios donde es necesario
- [ ] Se actualizó el README si aplica
- [ ] Se probaron las queries con datos de ejemplo
- [ ] Los scripts mantienen idempotencia

## 🐛 Reportar bugs

Abre un Issue en GitHub con:
- Descripción del problema
- Pasos para reproducir
- Comportamiento esperado vs actual
- Versión de PostgreSQL utilizada
- Scripts relevantes

## 💡 Sugerir mejoras

Abre un Issue en GitHub con:
- Descripción de la mejora
- Caso de uso
- Beneficios esperados
- Implementación propuesta (opcional)

¡Gracias por contribuir! 🎉
