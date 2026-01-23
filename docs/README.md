# 📋 Pipeline ETL - Google Drive Desktop + Snowflake

**Versión:** 1.0  
**Fecha:** 2026-01-22  
**Ubicación:** `C:\Ciencia de Datos\Proceso_Snowflake`

---

## 🎯 Objetivo

Pipeline ETL completo para procesar datos de SQL Server → Google Drive Desktop → Snowflake, con visualización en Streamlit.

**Países soportados:** Chile, Colombia, Ecuador, Perú

---

## 📁 Estructura del Proyecto

```
Proceso_Snowflake/
├── etl/
│   ├── .env.template          # Plantilla de configuración
│   ├── .env                   # TU configuración (gitignore)
│   ├── 2_normalizar_headers.py
│   ├── 3_renombrar_archivos.py
│   └── 4_cargar_snowflake.py
├── streamlit/
│   └── app_reportes.py        # Dashboard Snowflake
├── logs/                       # Logs de ejecución
└── docs/
    └── README.md              # Este archivo
```

---

## 🚀 Setup Inicial

### 1. Instalar Google Drive Desktop

1. Descarga: https://www.google.com/drive/download/
2. Instala y configura tu cuenta Google
3. Crea carpeta en Drive: `ETL_Snowflake/`
4. Dentro crea: `CHILE/`, `COLOMBIA/`, `ECUADOR/`, `PERU/`
5. Anota la ruta local (ejemplo: `G:\Mi unidad\ETL_Snowflake`)

### 2. Configurar Variables de Entorno

```powershell
cd "C:\Ciencia de Datos\Proceso_Snowflake\etl"

# Copiar plantilla
Copy-Item .env.template .env

# Editar con tus credenciales
notepad .env
```

**Variables obligatorias:**
```dotenv
DRIVE_BASE_DIR=G:\Mi unidad\ETL_Snowflake
PAISES_FOLDERS=CHILE,COLOMBIA,ECUADOR,PERU

SNOWFLAKE_ACCOUNT=tu_account.region
SNOWFLAKE_USER=tu_usuario
SNOWFLAKE_PASSWORD=tu_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=DEV_LND
SNOWFLAKE_SCHEMA=_SQL_CHI
SNOWFLAKE_ROLE=tu_role
```

### 3. Instalar Dependencias

```powershell
cd "C:\Ciencia de Datos\Proceso_Snowflake"

# Crear entorno virtual (opcional pero recomendado)
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Instalar paquetes
pip install polars pandas pyarrow snowflake-connector-python python-dotenv streamlit
```

---

## 📊 Pipeline - Flujo Completo

### Paso 1: Descargar desde SQL Server (Streamlit)

**Herramienta:** `1_descargar_sql_server.py`

```powershell
cd "C:\Ciencia de Datos\Proceso_Snowflake"
streamlit run etl/1_descargar_sql_server.py
```

**11 Reportes Disponibles:**
1. 📊 **Reporte Único de Ventas** (con EAN automático)
2. 📈 **Reporte Ventas Sellin** (con EAN automático)
3. 🏪 **Reporte Ventas Mercado** (con EAN automático)
4. 👥 **Listar Clientes**
5. 📦 **Listar Productos Detallado**
6. 📋 **Stock Almacén y Lote**
7. 💰 **Precio Lista**
8. 💵 **Reporte Cartera**
9. 📑 **Documento Vta Detallada** (con EAN automático)
10. 💲 **Diferencia Precios**
11. 📊 **Fill Rate Cliente-Producto**

**Columna EAN:**
- ✅ Se agrega automáticamente en reportes de ventas (1, 2, 3, 9)
- ✅ JOIN directo a `maeGC_ProductoEquiv` durante la descarga
- ✅ Sin necesidad de pasos adicionales

**Acciones:**
1. Selecciona país(es)
2. Selecciona reporte
3. Ingresa fechas (si aplica)
4. Ejecuta → Guarda automáticamente en Drive: `G:\Mi unidad\ETL_Snowflake\{PAIS}\`

### Paso 2: Normalizar Headers CSV

**Script:** `2_normalizar_headers.py`

```powershell
cd "C:\Ciencia de Datos\Proceso_Snowflake\etl"

# Dry-run (ver cambios sin aplicar)
python 2_normalizar_headers.py --dry-run

# Ejecutar normalización
python 2_normalizar_headers.py
```

**Transformaciones:**
- `Cod. Cliente` → `COD_CLIENTE`
- `Razón Social` → `RAZON_SOCIAL`
- `Número Cliente` → `NUMERO_CLIENTE`
- Etc. (ver mapeo completo en script)

**Salida:** Archivos `*_normalizado.csv` en cada carpeta país

### Paso 3: Renombrar Archivos

**Script:** `3_renombrar_archivos.py`

```powershell
# Dry-run (ver cambios)
python 3_renombrar_archivos.py

# Ejecutar renombrado
python 3_renombrar_archivos.py --apply
```

**Transformaciones:**
- Remueve timestamps: `_20260122_143025`
- Garantiza sufijo país: `_CHILE_normalizado.csv`
- Limpia guiones bajos múltiples

### Paso 4: Cargar a Snowflake

**Script:** `4_cargar_snowflake.py`

```powershell
python 4_cargar_snowflake.py
```

**Proceso:**
1. Detecta separador CSV automáticamente (`;` o `,`)
2. Lee con Pandas/Polars
3. **Crea backup:** `{TABLA}` → `{TABLA}_OLD`
4. Crea tabla nueva con estructura del CSV
5. Convierte a Parquet
6. Carga con `PUT` + `COPY INTO`

**IMPORTANTE:**
- ✅ Backups automáticos protegen datos existentes
- ✅ Columna EAN garantizada en la carga
- ✅ Nombres de tabla sin acentos (Único → UNICO)

**Logs:** Guardados en `Proceso_Snowflake/logs/`

---

## 📊 Streamlit - Dashboard

### Ejecutar Localmente

```powershell
cd "C:\Ciencia de Datos\Proceso_Snowflake"
streamlit run streamlit/app_reportes.py
```

Abre: http://localhost:8501

### Desplegar en Streamlit Cloud

1. Sube proyecto a GitHub (excluir `.env`)
2. En https://share.streamlit.io → "New app"
3. Selecciona repo y archivo: `streamlit/app_reportes.py`
4. Configura secrets en Settings → Secrets:

```toml
[snowflake]
account = "tu_account.region"
user = "tu_usuario"
password = "tu_password"
warehouse = "COMPUTE_WH"
database = "DEV_LND"
schema = "_SQL_CHI"
role = "tu_role"
```

5. Deploy

### Desplegar en Streamlit in Snowflake

```sql
-- En Snowflake Worksheet
USE DATABASE DEV_LND;
USE SCHEMA _SQL_CHI;

-- Crear Streamlit app
CREATE STREAMLIT APP_REPORTES_MULTIPAIS
  FROM '@~/streamlit'
  MAIN_FILE = 'app_reportes.py';

-- Subir archivo
PUT file://C:\Ciencia de Datos\Proceso_Snowflake\streamlit\app_reportes.py @~/streamlit/;

-- Ejecutar
ALTER STREAMLIT APP_REPORTES_MULTIPAIS SET COMMENT = 'Dashboard Multi-País';
```

---

## 🔧 Comandos Útiles

### Verificar Estado de Tablas en Snowflake

```sql
-- Ver todas las tablas (incluyendo backups)
SHOW TABLES IN SCHEMA _SQL_CHI;

-- Ver solo tablas activas (sin _OLD)
SHOW TABLES LIKE '%CHILE' IN SCHEMA _SQL_CHI;

-- Verificar columnas de una tabla
DESC TABLE REPORTE_UNICO_DE_VENTAS_CHILE;

-- Ver backups
SHOW TABLES LIKE '%_OLD' IN SCHEMA _SQL_CHI;
```

### Restaurar desde Backup

```sql
-- Si necesitas restaurar tabla desde _OLD
DROP TABLE REPORTE_UNICO_DE_VENTAS_CHILE;
ALTER TABLE REPORTE_UNICO_DE_VENTAS_CHILE_OLD 
  RENAME TO REPORTE_UNICO_DE_VENTAS_CHILE;
```

### Limpiar Backups Antiguos

```sql
-- Eliminar todas las tablas _OLD
DROP TABLE IF EXISTS REPORTE_UNICO_DE_VENTAS_CHILE_OLD;
DROP TABLE IF EXISTS LISTAR_CLIENTES_CHILE_OLD;
-- ... etc
```

---

## 🐛 Troubleshooting

### Error: "Google Drive no detectado"

**Causa:** Ruta incorrecta o Drive Desktop no instalado

**Solución:**
```powershell
# Verificar ruta
Test-Path "G:\Mi unidad\ETL_Snowflake"

# Actualizar .env con ruta correcta
notepad etl\.env
```

### Error: "Columna EAN no se carga"

**Causa:** CSV mal formado o separador incorrecto

**Solución:**
- ✅ Script detecta separador automáticamente
- ✅ Usa Pandas con `on_bad_lines='skip'`
- ✅ Backup automático protege tabla existente

### Error: "Tabla con acentos"

**Causa:** Nombre de archivo con caracteres especiales

**Solución:**
- ✅ Script normaliza automáticamente: Único → UNICO
- ✅ Remueve emojis y símbolos

### Error de conexión Snowflake

**Causa:** Credenciales incorrectas o red

**Solución:**
```powershell
# Verificar credenciales
python -c "from dotenv import load_dotenv; import os; load_dotenv('etl/.env'); print(os.getenv('SNOWFLAKE_ACCOUNT'))"

# Probar conexión
python -c "import snowflake.connector; snowflake.connector.connect(...)"
```

---

## 📈 Mejoras Futuras

- [ ] Automatizar descarga SQL Server → Drive (script programado)
- [ ] Alertas por email en caso de errores
- [ ] Dashboard con métricas de calidad de datos
- [ ] Integración con Apache Airflow para orquestación
- [ ] Migración a Google Cloud Storage (GCS) para External Stages nativos

---

## 📞 Soporte

**Ubicación código:** `C:\Ciencia de Datos\Proceso_Snowflake`  
**Logs:** `Proceso_Snowflake/logs/`  
**Documentación adicional:** `C:\Ciencia de Datos\AGENTS.MD`

---

## 🔐 Seguridad

⚠️ **NUNCA commitear `.env` a Git**

Agregar a `.gitignore`:
```gitignore
.env
*.log
logs/
.venv/
__pycache__/
```

---

## 📝 Changelog

**v1.0 (2026-01-22)**
- ✅ Configuración Google Drive Desktop
- ✅ Pipeline ETL completo (4 pasos)
- ✅ Backups automáticos (_OLD)
- ✅ Detección automática de separador CSV
- ✅ Dashboard Streamlit multi-país
- ✅ Documentación completa

---

*Última actualización: 2026-01-22*
