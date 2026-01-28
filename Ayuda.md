# 📘 Guía de Ayuda - Genomma Descargas

## 📋 Tabla de Contenidos

1. [Introducción](#introducción)
2. [Configuración Inicial](#configuración-inicial)
3. [Actualizar desde/hacia GitHub](#actualizar-desdehacia-github)
4. [Uso de la Aplicación](#uso-de-la-aplicación)
5. [Entornos: Codespaces vs Windows](#entornos-codespaces-vs-windows)
6. [Procedimientos Específicos](#procedimientos-específicos)
7. [Resolución de Problemas](#resolución-de-problemas)

---

## 🎯 Introducción

Esta aplicación Streamlit permite:
- 📥 Descargar datos desde Snowflake
- 📊 Ejecutar reportes de SQL Server
- 🔄 Procesamiento ETL automático
- 📈 Análisis de datos de múltiples países (Chile, Colombia, Ecuador, Perú)

La aplicación puede ejecutarse en:
- **GitHub Codespaces** (servidor Linux remoto)
- **PC Windows Local** (con Google Drive Desktop opcional)

---

## ⚙️ Configuración Inicial

### En GitHub Codespaces:

1. **Abrir el repositorio en Codespaces:**
   - Ir a: https://github.com/oaf2023/genomma_descargas
   - Click en el botón verde `<> Code`
   - Seleccionar pestaña `Codespaces`
   - Click en `Create codespace on main`

2. **Esperar a que se instalen las dependencias:**
   ```bash
   # El contenedor instalará automáticamente:
   # - Python 3.11
   # - Streamlit
   # - pandas, snowflake-connector-python
   # - Otras dependencias del requirements.txt
   ```

3. **Iniciar la aplicación:**
   ```bash
   streamlit run streamlit_app.py
   ```

### En Windows Local:

1. **Clonar el repositorio:**
   ```bash
   git clone https://github.com/oaf2023/genomma_descargas.git
   cd genomma_descargas
   ```

2. **Crear entorno virtual:**
   ```bash
   python -m venv venv
   venv\Scripts\activate
   ```

3. **Instalar dependencias:**
   ```bash
   pip install -r requirements.txt
   
   # Para SQL Server, instalar también:
   pip install pyodbc
   # Descargar ODBC Driver 18 for SQL Server desde Microsoft
   ```

4. **Ejecutar aplicación:**
   ```bash
   streamlit run streamlit_app.py
   ```

---

## 🔄 Actualizar desde/hacia GitHub

### 📥 Actualizar DESDE GitHub (Pull)

Cuando alguien más sube cambios al repositorio:

```bash
# 1. Ir al directorio del proyecto
cd /workspaces/genomma_descargas

# 2. Verificar en qué rama estás
git branch

# 3. Actualizar desde GitHub
git pull origin main

# 4. Ver qué archivos cambiaron
git log --oneline -5
```

**En Streamlit:** Si actualizas archivos mientras la app está corriendo, debes **recargar la página** para ver los cambios.

### 📤 Actualizar HACIA GitHub (Push)

Cuando haces cambios locales y quieres subirlos:

```bash
# 1. Ver qué archivos cambiaron
git status

# 2. Agregar archivos al staging
git add .                          # Agregar todos los archivos
# O específicos:
git add archivo1.py archivo2.csv

# 3. Hacer commit con mensaje descriptivo
git commit -m "Descripción clara de los cambios"

# 4. Subir a GitHub
git push origin main

# 5. Verificar en GitHub que los cambios están
# Ir a: https://github.com/oaf2023/genomma_descargas
```

### 📝 Buenos mensajes de commit:

✅ **Buenos ejemplos:**
- `"Agregar validación de fechas en reportes SQL"`
- `"Corregir error de conexión en Snowflake"`
- `"Actualizar lista de tablas a descargar"`

❌ **Malos ejemplos:**
- `"cambios"`
- `"fix"`
- `"update"`

### 🔀 Resolver conflictos:

Si hay conflictos al hacer pull:

```bash
# 1. Git te avisará qué archivos tienen conflictos
git status

# 2. Abrir archivos en VS Code
# Buscar marcadores: <<<<<<< HEAD

# 3. Editar manualmente o usar herramienta de VS Code
# "Accept Current Change", "Accept Incoming Change", etc.

# 4. Después de resolver:
git add archivo_resuelto.py
git commit -m "Resolver conflictos de merge"
git push origin main
```

---

## 🖥️ Uso de la Aplicación

### Menú Principal

La aplicación tiene 6 opciones:

#### 1️⃣ **Inicio**
- Página de bienvenida
- Información general
- Sin funcionalidades específicas

#### 2️⃣ **Parámetros de Conexión**
- Configurar credenciales de Snowflake
- Variables de entorno necesarias:
  - `SNOWFLAKE_USER`
  - `SNOWFLAKE_PASSWORD`
  - `SNOWFLAKE_ACCOUNT`
  - `SNOWFLAKE_WAREHOUSE`
  - `SNOWFLAKE_DATABASE`
  - `SNOWFLAKE_SCHEMA`

#### 3️⃣ **Descargar Datos**
- Descargar datos de Snowflake
- Filtros por fecha
- Exportar a CSV/Excel
- Previsualización de datos

#### 4️⃣ **Pipeline ETL**
- Procesos ETL automatizados
- Transformación de datos
- Validación de calidad

#### 5️⃣ **Reportes y Análisis**
- Reportes preconstruidos
- Visualizaciones
- Dashboards interactivos

#### 6️⃣ **📈 Reportes SQL Server** ⭐

Esta es la sección más completa para trabajar con SQL Server.

**Características:**
- Ejecutar **14 stored procedures** diferentes
- Descargar **tablas completas** (últimos 36 meses)
- Soporte **multi-país** (Chile, Colombia, Ecuador, Perú)
- Control de integridad con **hashing**
- Exportación a **CSV y Excel**

**Reportes disponibles:**
1. Reporte Ventas Por Cliente
2. Reporte Productos Más Vendidos
3. Reporte Inventario Actual
4. Reporte Documentos Por Distribución
5. Reporte Facturas Por Periodo
6. Reporte Clientes Activos
7. Reporte Top 10 Productos
8. Reporte Ventas Por Marca
9. Reporte Devoluciones
10. Reporte Estados de Documentos
11. Reporte Tipos de Documento
12. Reporte Conceptos
13. Reporte Productos Con EAN
14. Reporte Customer Master (RM00101)

---

## 🌍 Entornos: Codespaces vs Windows

### 🐧 GitHub Codespaces (Linux)

**Características:**
- Servidor remoto en la nube
- Acceso desde cualquier navegador
- Archivos temporales en `/tmp/genomma_reportes`
- ⚠️ **Los archivos se borran al cerrar Codespaces**

**Flujo de descarga:**

```
SQL Server → Codespaces (/tmp) → Botón ZIP → Tu PC → Google Drive (manual)
```

**Pasos:**
1. Click en "⬇️ Descargar Tablas Base"
2. Esperar a que termine (ver barra de progreso)
3. Click en "📦 Descargar TODOS los archivos (ZIP)"
4. Guardar ZIP en tu PC
5. Descomprimir y copiar a Google Drive si lo necesitas

**Pros:**
- ✅ No necesita instalación local
- ✅ Mismo entorno para todos
- ✅ No usa recursos de tu PC

**Contras:**
- ❌ Requiere conexión a Internet
- ❌ Archivos temporales (debes descargar)
- ❌ No hay pyodbc preinstalado (no funciona SQL Server por defecto)

### 🪟 Windows Local

**Características:**
- Ejecución en tu PC
- Archivos persistentes
- Integración con Google Drive Desktop automática
- pyodbc y drivers ODBC disponibles

**Flujo de descarga:**

```
SQL Server → PC Windows → Google Drive Desktop (automático)
```

**Pasos:**
1. Click en "⬇️ Descargar Tablas Base"
2. Los archivos se guardan en:
   - `G:\Mi unidad\ETL_Snowflake` (si tienes Google Drive)
   - `C:\Ciencia de Datos\otros_datos` (si no)
3. ✅ Ya están sincronizados con Google Drive

**Pros:**
- ✅ Archivos permanentes
- ✅ Sincronización automática con Google Drive
- ✅ SQL Server funciona nativamente
- ✅ Sin límites de tiempo

**Contras:**
- ❌ Requiere instalación local
- ❌ Configuración de drivers ODBC
- ❌ Usa recursos de tu PC

---

## 📋 Procedimientos Específicos

### 📥 Descargar Tablas Base (SQL Server)

**Requisitos previos:**
- Archivo `tablas_a_descargar.csv` en el repositorio
- Conexión a SQL Server configurada
- pyodbc instalado (solo en Windows local)

**Pasos:**

1. **Ir a "📈 Reportes SQL Server"**

2. **Verificar información del entorno:**
   - Expandir "ℹ️ Información del Entorno"
   - Leer instrucciones según tu entorno

3. **Configurar:**
   - Seleccionar países (multiselección)
   - Elegir reporte (opcional para descarga de tablas)

4. **Descargar:**
   - Click en "⬇️ Descargar Tablas Base"
   - Verás:
     - ✅ Verificación de pyodbc
     - 📂 Directorio de destino
     - 🔌 Prueba de conexión
     - 📊 Progreso tabla por tabla
     - ✅ Resumen al final

5. **Obtener archivos:**
   - **Codespaces:** Click en botón ZIP
   - **Windows:** Archivos ya guardados en Google Drive

### 🔄 Ejecutar Stored Procedures

**Pasos:**

1. **Seleccionar país(es)**

2. **Elegir reporte del dropdown**
   - Verás descripción y parámetros

3. **Configurar parámetros** (si el reporte los requiere)
   - Fechas de inicio/fin
   - Otros filtros específicos

4. **Click en "▶️ Ejecutar Reporte"**
   - Verás:
     - Spinner de ejecución
     - Resultados en tabla
     - Botones de descarga (CSV/Excel)

5. **Descargar resultados:**
   - Click en "📥 Descargar CSV" o "📥 Descargar Excel"
   - En Codespaces: descarga directa al navegador
   - En Windows: puede guardar en Google Drive o descargar

### 📄 Agregar Nuevas Tablas

Para agregar tablas a la descarga automática:

1. **Editar `tablas_a_descargar.csv`:**
   ```csv
   nombre_tabla
   movGC_DocumentoxDistribucion
   movGC_vtDocumentoVtaCab
   nueva_tabla_aqui
   ```

2. **Verificar nombre exacto de la tabla:**
   - Conectar a SQL Server Management Studio
   - Ejecutar: `SELECT * FROM INFORMATION_SCHEMA.TABLES`
   - Copiar nombre exacto (case-sensitive)

3. **Guardar y commit:**
   ```bash
   git add tablas_a_descargar.csv
   git commit -m "Agregar nueva_tabla a lista de descarga"
   git push origin main
   ```

4. **En otros entornos, hacer pull:**
   ```bash
   git pull origin main
   ```

### 🔐 Configurar Conexión SQL Server

**Editar configuración en `app_reportes_sql.py`:**

```python
SERVERS_CONFIG = {
    'CHILE': {
        'server': r'IBMSQLN1\DynamicsChile',
        'database': 'GPCPR',
        'user': 'usuario',
        'password': 'contraseña'
    },
    # ... otros países
}
```

⚠️ **IMPORTANTE:** No subir contraseñas a GitHub. Usar variables de entorno:

```python
import os

SERVERS_CONFIG = {
    'CHILE': {
        'server': r'IBMSQLN1\DynamicsChile',
        'database': 'GPCPR',
        'user': os.getenv('SQL_USER_CHILE'),
        'password': os.getenv('SQL_PASSWORD_CHILE')
    }
}
```

Configurar en Codespaces:
```bash
# En Settings → Secrets → Codespaces
# Agregar: SQL_USER_CHILE, SQL_PASSWORD_CHILE, etc.
```

---

## 🔧 Resolución de Problemas

### ❌ "pyodbc no está instalado"

**Problema:** La app dice que pyodbc no está disponible.

**Solución en Windows:**
```bash
pip install pyodbc
# Descargar e instalar: ODBC Driver 18 for SQL Server
# https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
```

**En Codespaces:**
- pyodbc no funciona en Codespaces por defecto
- Necesitas ejecutar la app en Windows local para SQL Server
- Alternativa: usar solo funciones de Snowflake en Codespaces

### ❌ "No se encontró tablas_a_descargar.csv"

**Problema:** Error al descargar tablas.

**Solución:**
```bash
# Verificar que existe:
ls -la tablas_a_descargar.csv

# Si no existe, hacer pull:
git pull origin main

# Verificar contenido:
cat tablas_a_descargar.csv
```

### ❌ "Error de conexión a SQL Server"

**Problema:** No puede conectar a SQL Server.

**Verificar:**
1. ✅ Drivers ODBC instalados
2. ✅ Conectividad de red (VPN si es necesario)
3. ✅ Credenciales correctas en `SERVERS_CONFIG`
4. ✅ Firewall permite conexión
5. ✅ Servidor y base de datos accesibles

**Test de conexión:**
```python
import pyodbc

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=IBMSQLN1\\DynamicsChile;"
    "DATABASE=GPCPR;"
    "UID=usuario;"
    "PWD=contraseña;"
    "TrustServerCertificate=yes"
)

try:
    conn = pyodbc.connect(conn_str)
    print("✅ Conexión exitosa")
    conn.close()
except Exception as e:
    print(f"❌ Error: {e}")
```

### ❌ "Los archivos no están en Google Drive"

**En Codespaces:**
- Google Drive NO se sincroniza automáticamente
- Debes descargar el ZIP y copiar manualmente

**En Windows:**
- Verificar que Google Drive Desktop está instalado
- Verificar que `G:\Mi unidad\ETL_Snowflake` existe
- Si no, los archivos están en `C:\Ciencia de Datos\otros_datos`

### ❌ Conflictos de Git

**Problema:** Git no permite pull o push.

**Solución:**
```bash
# Ver estado:
git status

# Si hay cambios locales no guardados:
git stash                    # Guardar temporalmente
git pull origin main         # Actualizar
git stash pop               # Recuperar cambios

# Si hay conflictos:
# Editar archivos manualmente
git add .
git commit -m "Resolver conflictos"
git push origin main
```

### 🐛 Streamlit no recarga cambios

**Problema:** Hice cambios pero la app no los refleja.

**Solución:**
1. En la app, click en ☰ (menú) → "Rerun"
2. O presionar `R` en el teclado
3. O refrescar el navegador (F5)
4. Si persiste: detener y reiniciar Streamlit

### 📊 Datos vacíos o erróneos

**Verificar:**
1. Filtros de fecha correctos (últimos 36 meses)
2. País seleccionado correcto
3. Tabla tiene datos en ese período
4. Columna de fecha existe en la tabla

**Test directo en SQL Server:**
```sql
-- Verificar datos existen:
SELECT COUNT(*) 
FROM tabla 
WHERE fecha_columna >= DATEADD(MONTH, -36, GETDATE())
```

---

## 📚 Recursos Adicionales

### 📖 Documentación:
- **Streamlit:** https://docs.streamlit.io
- **Pandas:** https://pandas.pydata.org/docs
- **pyodbc:** https://github.com/mkleehammer/pyodbc/wiki
- **Git:** https://git-scm.com/doc

### 🔗 Enlaces Útiles:
- **Repositorio GitHub:** https://github.com/oaf2023/genomma_descargas
- **ODBC Driver:** https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
- **Google Drive Desktop:** https://www.google.com/drive/download

### 📞 Soporte:
- Crear un **Issue** en GitHub para reportar bugs
- Documentar el problema con capturas de pantalla
- Incluir logs de error completos

---

## 🎓 Mejores Prácticas

### ✅ Git:
- Hacer commits frecuentes con mensajes descriptivos
- Pull antes de empezar a trabajar
- Push al terminar el día
- Nunca subir contraseñas o credenciales

### ✅ Código:
- Comentar cambios importantes
- Validar datos antes de procesar
- Manejar errores con try/except
- Logs informativos en consola

### ✅ Datos:
- Siempre descargar archivos en Codespaces antes de cerrar
- Verificar datos descargados tienen sentido
- Mantener backups de archivos importantes
- Documentar transformaciones de datos

### ✅ Seguridad:
- No compartir credenciales en código
- Usar variables de entorno
- No subir archivos con datos sensibles a GitHub
- Verificar .gitignore incluye archivos de datos

---

**Última actualización:** Enero 2026  
**Versión de la app:** 1.0  
**Mantenedor:** Sistema Genomma
