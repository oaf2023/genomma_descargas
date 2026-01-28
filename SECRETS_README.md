## 🔐 Configuración de Secrets para Streamlit

Este repositorio contiene dos formas de configurar las credenciales de Snowflake:

### 📁 Archivos de configuración:

1. **`.streamlit/secrets.toml`** - Para ejecución LOCAL
2. **`secrets_streamlit_cloud.toml`** - Para STREAMLIT CLOUD
3. **`etl/.env`** - Variables de entorno (alternativo)

---

## 🌐 Para Streamlit Cloud (share.streamlit.io)

### Paso 1: Copia el contenido

Abre el archivo [`secrets_streamlit_cloud.toml`](secrets_streamlit_cloud.toml) y copia SOLO esta parte:

```toml
[snowflake]
account = "QOB68501-GENOMMALAB"
user = "OAFONTANA"
password = "Familiafontana2025##"
warehouse = "GENOMMA"
database = "DEV_LND"
schema = "_SQL_CHI"
role = "OAFONTANA_ROLE"
```

### Paso 2: Pegar en Streamlit Cloud

1. Ve a https://share.streamlit.io
2. Abre tu aplicación
3. Click en **Settings** ⚙️ (esquina superior derecha)
4. Click en **Secrets**
5. Pega el contenido copiado
6. Click en **Save**

---

## 💻 Para ejecución LOCAL

El archivo `.streamlit/secrets.toml` ya está configurado y la app lo usará automáticamente.

---

## 🔄 Alternativa: Variables de entorno

Si prefieres usar variables de entorno, edita `etl/.env`:

```bash
SNOWFLAKE_ACCOUNT=QOB68501-GENOMMALAB
SNOWFLAKE_USER=OAFONTANA
SNOWFLAKE_PASSWORD=Familiafontana2025##
SNOWFLAKE_WAREHOUSE=GENOMMA
SNOWFLAKE_DATABASE=DEV_LND
SNOWFLAKE_SCHEMA=_SQL_CHI
SNOWFLAKE_ROLE=OAFONTANA_ROLE
```

---

## ✅ Verificar la conexión

1. Inicia la aplicación
2. Ve al menú lateral → **⚙️ Configuración**
3. Verás el estado de la conexión
4. Click en **"🔄 Probar Conexión"** para verificar

---

## 🔒 Seguridad

- ✅ Todos estos archivos están en `.gitignore`
- ✅ NO se subirán a GitHub
- ⚠️ Nunca compartas tus credenciales

---

**Última actualización:** 2026-01-27
