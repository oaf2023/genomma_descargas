# 🚀 Quick Start - Pipeline ETL Snowflake

**Ubicación:** `C:\Ciencia de Datos\Proceso_Snowflake`

---

## ⚡ Inicio Rápido (5 minutos)

### 1️⃣ Instalar Google Drive Desktop
- Descarga: https://www.google.com/drive/download/
- Crea carpeta: `ETL_Snowflake/` con subcarpetas `CHILE/`, `COLOMBIA/`, `ECUADOR/`, `PERU/`

### 2️⃣ Configurar Credenciales
```powershell
cd "C:\Ciencia de Datos\Proceso_Snowflake\etl"
Copy-Item .env.template .env
notepad .env  # Completa tus credenciales
```

### 3️⃣ Instalar Dependencias
```powershell
pip install -r requirements.txt
```

### 4️⃣ Ejecutar Pipeline
```powershell
# Simular (ver qué haría sin ejecutar)
python pipeline_maestro.py --dry-run

# Ejecutar completo
python pipeline_maestro.py

# Ejecutar solo carga a Snowflake
python pipeline_maestro.py --step 3
```

---

## 📊 Ver Resultados en Streamlit

```powershell
streamlit run streamlit/app_reportes.py
```

Abre: http://localhost:8501

---

## 📁 Estructura de Archivos

```
Proceso_Snowflake/
├── pipeline_maestro.py         ← SCRIPT PRINCIPAL (pasos 2-4)
├── etl/
│   ├── .env                    ← TU configuración
│   ├── 1_descargar_sql_server.py  ← PASO 1 (Streamlit)
│   ├── 2_normalizar_headers.py
│   ├── 3_renombrar_archivos.py
│   └── 4_cargar_snowflake.py
├── streamlit/
│   └── app_reportes.py         ← Dashboard
└── docs/
    └── README.md               ← Documentación completa
```

---

## 🔄 Flujo Completo

1. **Descarga SQL Server** → Google Drive
   ```powershell
   streamlit run etl/1_descargar_sql_server.py
   ```
2. **Pipeline ETL** → `python pipeline_maestro.py`
   - Normaliza headers CSV
   - Renombra archivos
   - Carga a Snowflake (con backups automáticos)
3. **Visualiza** → `streamlit run streamlit/app_reportes.py`

---

## 🆘 Ayuda Rápida

**Error "Google Drive no detectado":**
```powershell
# Verifica ruta en .env
Test-Path "G:\Mi unidad\ETL_Snowflake"
```

**Ver logs:**
```powershell
Get-Content logs\pipeline_*.log -Tail 50
```

**Documentación completa:**
- [docs/README.md](docs/README.md)

---

## ✅ Checklist Primera Ejecución

- [ ] Google Drive Desktop instalado
- [ ] Carpetas creadas: `CHILE/`, `COLOMBIA/`, `ECUADOR/`, `PERU/`
- [ ] Archivo `.env` configurado
- [ ] Dependencias instaladas: `pip install -r requirements.txt`
- [ ] **Paso 1:** CSVs descargados: `streamlit run etl/1_descargar_sql_server.py`
- [ ] **Pasos 2-4:** Pipeline ejecutado: `python pipeline_maestro.py`
- [ ] Streamlit funcionando: `streamlit run streamlit/app_reportes.py`

---

**¿Listo?** → `python pipeline_maestro.py --dry-run` 🚀
