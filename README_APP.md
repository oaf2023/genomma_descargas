# 🌎 Genomma Lab - Dashboard Snowflake

Aplicación Streamlit para consultar y analizar datos de Snowflake.

## 🚀 Inicio Rápido

### 1. Configurar credenciales

Edita el archivo `etl/.env` con tus credenciales de Snowflake:

```bash
SNOWFLAKE_ACCOUNT=tu_account.region
SNOWFLAKE_USER=tu_usuario
SNOWFLAKE_PASSWORD=tu_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=DEV_LND
SNOWFLAKE_SCHEMA=_SQL_CHI
SNOWFLAKE_ROLE=tu_role
```

### 2. Iniciar la aplicación

**MÉTODO RECOMENDADO:**
```bash
cd /workspaces/genomma_descargas
streamlit run streamlit/app_reportes.py --server.port 8501 --server.address 0.0.0.0
```

**Usando el script (alternativo):**
```bash
./run_app.sh
```

### 3. Acceder a la aplicación

- **URL local:** http://localhost:8501
- **URL pública (Codespaces):** Usa el puerto forwarding de VS Code

> **Nota:** Si usas GitHub Codespaces, VS Code abrirá automáticamente el puerto 8501 y te dará una URL pública.

## 📋 Funcionalidades

### 🏠 Inicio
- Dashboard con resumen de tablas disponibles
- Estado de conexión a Snowflake
- Acceso rápido a todas las funciones

### 📊 Consultar Datos
- Explora tablas por país (Chile, Colombia, Ecuador, Perú)
- Visualiza datos en tiempo real
- Aplica filtros personalizados
- Descarga datos en formato CSV
- Análisis estadístico básico

### 🔍 Query SQL
- Ejecuta consultas SQL personalizadas
- Editor de código SQL
- Resultados en formato tabla
- Exportación de resultados

### ⚙️ Configuración
- Verifica el estado de la conexión
- Revisa la configuración actual
- Prueba la conexión a Snowflake

## 🛠️ Requisitos

- Python 3.11+
- Streamlit
- Snowflake Connector
- Pandas
- Python-dotenv

Instalar dependencias:
```bash
pip install -r requirements.txt
```

## 📁 Estructura del Proyecto

```
genomma_descargas/
├── streamlit/
│   └── app_reportes.py      # Aplicación principal
├── etl/
│   ├── .env                 # Credenciales (NO subir a Git)
│   └── .env.template        # Plantilla de configuración
├── run_app.sh              # Script de inicio
├── requirements.txt        # Dependencias Python
└── README_APP.md          # Esta documentación
```

## 🔧 Solución de Problemas

### La aplicación no inicia

1. Verifica que no haya otro proceso usando el puerto 8501:
   ```bash
   lsof -ti:8501 | xargs kill -9
   ```

2. Reinicia la aplicación:
   ```bash
   ./run_app.sh
   ```

### Error de conexión a Snowflake

1. Verifica las credenciales en `etl/.env`
2. Ve a **⚙️ Configuración** en la app
3. Usa el botón "Probar Conexión"

### La aplicación se ve en blanco

1. Limpia la caché del navegador
2. Recarga la página (Ctrl+F5)
3. Verifica los logs:
   ```bash
   tail -f /tmp/streamlit.log
   ```

## 📞 Soporte

Para problemas o preguntas, contacta al equipo de desarrollo.

---

**Última actualización:** 2026-01-27
