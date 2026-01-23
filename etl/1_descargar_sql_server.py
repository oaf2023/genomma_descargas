"""
Aplicación Streamlit para descargar datos desde SQL Server a Google Drive
Adaptado para pipeline ETL con Google Drive Desktop

📂 Guarda automáticamente en Google Drive para posterior procesamiento con Snowflake
✅ Incluye descarga de 11 tablas base con columna EAN en movGC_vtDocumentoVtaDet

Autor: Sistema  
Fecha: 2025-01-22
"""

import streamlit as st
import pandas as pd
import pyodbc
from datetime import datetime, timedelta
import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar configuración
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)

st.set_page_config(
    page_title="Descarga SQL Server → Drive",
    page_icon="📥",
    layout="wide"
)

# ==========================================================================
# CONFIGURACIÓN
# ==========================================================================

DRIVE_BASE_DIR = os.getenv("DRIVE_BASE_DIR", r"G:\Mi unidad\ETL_Snowflake")
PAISES_STR = os.getenv("PAISES_FOLDERS", "CHILE,COLOMBIA,ECUADOR,PERU")
PAISES = [p.strip() for p in PAISES_STR.split(",") if p.strip()]

# Fallback a local
if not Path(DRIVE_BASE_DIR).exists():
    st.warning(f"⚠️ Google Drive no detectado en: {DRIVE_BASE_DIR}")
    st.info("💡 Usando carpeta local temporal. Instala Google Drive Desktop para sincronización automática.")
    DRIVE_BASE_DIR = r"C:\Ciencia de Datos\Proceso_Snowflake\temp_data"
    Path(DRIVE_BASE_DIR).mkdir(parents=True, exist_ok=True)

BASE_DIR = Path(DRIVE_BASE_DIR)

# Crear carpetas por país
for pais in PAISES:
    (BASE_DIR / pais).mkdir(parents=True, exist_ok=True)

# Configuración SQL Server
SERVERS_CONFIG = {
    'CHILE': {
        'server': r'IBMSQLN1\DynamicsChile',
        'database': 'GPCPR',
        'user': 'rdgp',
        'password': 'P3muGP@386x'
    },
    'COLOMBIA': {
        'server': r'IBMSQLN1\DynamicsColombia',
        'database': 'GPCOP',
        'user': 'rdgp',
        'password': 'P3muGP@386x'
    },
    'ECUADOR': {
        'server': r'IBMSQLN1\DynamicsEcuador',
        'database': 'GPECP',
        'user': 'rdgp',
        'password': 'P3muGP@386x'
    },
    'PERU': {
        'server': r'IBMSQLN1\DynamicsPeru',
        'database': 'GPPER',
        'user': 'rdgp',
        'password': 'P3ruGP@386x'
    }
}

DRIVER = 'ODBC Driver 18 for SQL Server'

# Tablas base a descargar
TABLAS_BASE = [
    "movGC_DocumentoxDistribucion",
    "movGC_vtDocumentoVtaCab",
    "movGC_vtDocumentoVtaDet",
    "maeGC_ProductoEquiv",
    "maeGC_cfEstado",
    "maeGC_cfTipoDocumento",
    "RM00101",
    "RM00201",
    "maeGC_cfConcepto",
    "maeGC_Producto",
    "maeGC_Marca"
]

# SPs disponibles
SPS_DISPONIBLES = {
    '📊 Reporte Único de Ventas': {
        'sp': 'uspGC_RptReporteUnicoDeVentasMACROS',
        'params': ['fecha_inicio', 'fecha_fin'],
        'desc': 'Documento de ventas completo',
        'agregar_ean': True
    },
    '📈 Reporte Ventas Sellin': {
        'sp': 'uspGC_RptReporteUnicoDeVentasSellinMACROS',
        'params': ['fecha_inicio', 'fecha_fin'],
        'desc': 'Ventas sell-in con detalle',
        'agregar_ean': True
    },
    '🏪 Reporte Ventas Mercado': {
        'sp': 'uspGC_RptReporteUnicoDeVentasMercadoMACROS',
        'params': ['fecha_inicio', 'fecha_fin'],
        'desc': 'Ventas por mercado',
        'agregar_ean': True
    },
    '👥 Listar Clientes': {
        'sp': 'uspGC_ListarClientesMACROS',
        'params': [],
        'desc': 'Listado completo de clientes',
        'agregar_ean': False
    },
    '📦 Listar Productos Detallado': {
        'sp': 'uspGC_ListarProductoDetalladoMACROS',
        'params': [],
        'desc': 'Catálogo de productos con equivalencias',
        'agregar_ean': False
    },
    '📋 Stock Almacén y Lote': {
        'sp': 'uspGC_ListarStockXAlmacenLoteMACROS',
        'params': [],
        'desc': 'Inventario por almacén y lote',
        'agregar_ean': False
    },
    '💰 Precio Lista': {
        'sp': 'uspGC_ObtenerPrecioListaMACROS',
        'params': [],
        'desc': 'Lista de precios activa',
        'agregar_ean': False
    },
    '💵 Reporte Cartera': {
        'sp': 'usp_ReporteCarteraMACROS',
        'params': ['fecha_inicio', 'fecha_fin'],
        'desc': 'Estado de cartera clientes',
        'agregar_ean': False
    },
    '📑 Documento Vta Detallada': {
        'sp': 'uspGC_ListarDocumentoVtaDetalladaMACROS',
        'params': ['fecha_inicio', 'fecha_fin'],
        'desc': 'Documentos de venta detallados',
        'agregar_ean': True
    },
    '💲 Diferencia Precios': {
        'sp': 'uspGC_ListarDiferenciaPreciosMACROS',
        'params': ['fecha_inicio', 'fecha_fin'],
        'desc': 'Análisis diferencias de precios',
        'agregar_ean': False
    },
    '📊 Fill Rate Cliente-Producto': {
        'sp': 'uspGC_ListarFillRateXClienteProductoMACROS',
        'params': ['fecha_inicio', 'fecha_fin'],
        'desc': 'Fill rate por cliente y producto',
        'agregar_ean': False
    }
}

# ==========================================================================
# FUNCIONES
# ==========================================================================

def get_connection(pais: str):
    """Establece conexión con SQL Server (sin cache)"""
    try:
        config = SERVERS_CONFIG[pais]
        conn_str = (
            f'DRIVER={{{DRIVER}}};'
            f'SERVER={config["server"]};'
            f'DATABASE={config["database"]};'
            f'UID={config["user"]};'
            f'PWD={config["password"]};'
            f'TrustServerCertificate=yes;'
            f'Timeout=300;'
        )
        return pyodbc.connect(conn_str, timeout=30)
    except Exception as e:
        st.error(f"❌ Error conectando a {pais}: {e}")
        return None


def desambiguar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    """Renombra columnas duplicadas agregando sufijos _1, _2, etc."""
    if df.empty:
        return df
    
    columnas = list(df.columns)
    contadores = {}
    nuevas_columnas = []
    
    for col in columnas:
        if col in contadores:
            contadores[col] += 1
            nuevas_columnas.append(f"{col}_{contadores[col]}")
        else:
            contadores[col] = 0
            nuevas_columnas.append(col)
    
    # Si hubo duplicados, renombrar y notificar
    if any(c != nc for c, nc in zip(columnas, nuevas_columnas)):
        duplicados = [c for c in contadores if contadores[c] > 0]
        st.warning(f"⚠️ Columnas duplicadas renombradas: {', '.join(duplicados)}")
        df.columns = nuevas_columnas
    
    return df


def agregar_columna_ean(df: pd.DataFrame, pais: str) -> pd.DataFrame:
    """Agrega columna EAN mediante JOIN a maeGC_ProductoEquiv"""
    if df.empty:
        df['EAN'] = ''
        return df
    
    # Buscar columna de producto
    col_producto = None
    posibles = ['Código de producto', 'CodigoProducto', 'cProducto', 
                'cProductoVta', 'ITEMNMBR', 'Codigo Producto']
    
    for nombre in posibles:
        if nombre in df.columns:
            col_producto = nombre
            break
    
    if not col_producto:
        df['EAN'] = ''
        st.warning(f"⚠️ {pais}: No se encontró columna de producto para EAN")
        return df
    
    conn = get_connection(pais)
    if not conn:
        df['EAN'] = ''
        return df
    
    cursor = conn.cursor()
    
    try:
        codigo_pais = {'CHILE': 'CL', 'PERU': 'PE', 'COLOMBIA': 'CO', 'ECUADOR': 'EC'}.get(pais, 'CL')
        
        cursor.execute("SET NOCOUNT ON")
        cursor.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        
        query_ean = f"""
        SELECT DISTINCT
            RTRIM(cProducto) AS cProducto,
            RTRIM(cProductoEquiv) AS EAN
        FROM dbo.maeGC_ProductoEquiv WITH (NOLOCK)
        WHERE cEquivalencia = 'EAN12' 
            AND cPais = '{codigo_pais}'
        OPTION (MAXDOP 4)
        """
        
        cursor.execute(query_ean)
        columns_ean = [col[0] for col in cursor.description]
        rows_ean = cursor.fetchall()
        
        if rows_ean and columns_ean:
            df_ean = pd.DataFrame.from_records(rows_ean, columns=columns_ean)
            
            df['_codigo_limpio'] = df[col_producto].astype(str).str.strip()
            df_ean['cProducto'] = df_ean['cProducto'].astype(str).str.strip()
            
            df = df.merge(
                df_ean[['cProducto', 'EAN']],
                left_on='_codigo_limpio',
                right_on='cProducto',
                how='left'
            )
            
            df.drop(columns=['_codigo_limpio', 'cProducto'], inplace=True, errors='ignore')
            df['EAN'] = df['EAN'].fillna('').astype(str)
            
            ean_count = (df['EAN'] != '').sum()
            st.success(f"✅ {pais}: {ean_count:,} códigos EAN agregados ({ean_count/len(df)*100:.1f}%)")
        else:
            df['EAN'] = ''
            st.info(f"ℹ️ {pais}: No se encontraron códigos EAN")
            
    except Exception as e:
        st.error(f"❌ {pais}: Error agregando EAN: {e}")
        df['EAN'] = ''
    finally:
        cursor.close()
        conn.close()
    
    return df


def ejecutar_sp(pais: str, sp_name: str, params: list = None, agregar_ean: bool = False) -> pd.DataFrame:
    """Ejecuta stored procedure con optimizaciones"""
    conn = get_connection(pais)
    if not conn:
        return None
    
    cursor = conn.cursor()
    
    try:
        cursor.execute("SET NOCOUNT ON")
        cursor.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        
        if params:
            placeholders = ', '.join(['?' for _ in params])
            query = f"EXEC {sp_name} {placeholders}"
            cursor.execute(query, params)
        else:
            cursor.execute(f"EXEC {sp_name}")
        
        columns = [col[0] for col in cursor.description] if cursor.description else []
        
        chunk_size = 10000
        all_rows = []
        while True:
            chunk = cursor.fetchmany(chunk_size)
            if not chunk:
                break
            all_rows.extend(chunk)
        
        if all_rows and columns:
            df = pd.DataFrame.from_records(all_rows, columns=columns)
            
            # Desambiguar columnas duplicadas
            df = desambiguar_columnas(df)
            
            if agregar_ean and 'EAN' not in df.columns:
                df = agregar_columna_ean(df, pais)
            
            return df
        return pd.DataFrame()
        
    except Exception as e:
        error_msg = str(e).lower()
        if "could not find stored procedure" in error_msg:
            st.warning(f"⚠️ {pais}: SP '{sp_name}' no existe")
        else:
            st.error(f"❌ {pais}: Error ejecutando SP - {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def detectar_columna_fecha(pais: str, tabla: str) -> str:
    """Detecta columna de fecha para filtro de 36 meses"""
    conn = get_connection(pais)
    if not conn:
        return None
    
    cursor = conn.cursor()
    
    try:
        query = f"""
        SELECT TOP 1 COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{tabla}'
        AND DATA_TYPE IN ('datetime', 'date', 'smalldatetime', 'datetime2')
        ORDER BY ORDINAL_POSITION
        """
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0] if result else None
    except:
        return None
    finally:
        cursor.close()
        conn.close()


def descargar_tabla(pais: str, tabla: str) -> pd.DataFrame:
    """Descarga tabla completa o filtrada por 36 meses (incluye EAN para movGC_vtDocumentoVtaDet)"""
    conn = get_connection(pais)
    if not conn:
        return None
    
    cursor = conn.cursor()
    
    try:
        columna_fecha = detectar_columna_fecha(pais, tabla)
        
        cursor.execute("SET NOCOUNT ON")
        cursor.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        
        codigo_pais = {'CHILE': 'CL', 'PERU': 'PE', 'COLOMBIA': 'CO', 'ECUADOR': 'EC'}.get(pais, 'CL')
        
        # Query especial para movGC_vtDocumentoVtaDet con EAN
        if tabla.upper() == 'MOVGC_VTDOCUMENTOVTADET':
            if columna_fecha:
                fecha_inicio = (datetime.now() - timedelta(days=36*30)).strftime('%Y-%m-%d')
                query = f"""
                WITH EAN_LOOKUP AS (
                    SELECT 
                        RTRIM(cProducto) AS cProducto,
                        RTRIM(cProductoEquiv) AS EAN
                    FROM dbo.maeGC_ProductoEquiv WITH (NOLOCK)
                    WHERE cEquivalencia = 'EAN12' 
                        AND cPais = '{codigo_pais}'
                )
                SELECT 
                    d.*,
                    COALESCE(e.EAN, '') AS EAN
                FROM {tabla} d WITH (NOLOCK, INDEX(0))
                LEFT JOIN EAN_LOOKUP e ON RTRIM(d.cProductoVta) = e.cProducto
                WHERE d.{columna_fecha} >= '{fecha_inicio}'
                OPTION (MAXDOP 4, OPTIMIZE FOR UNKNOWN)
                """
            else:
                query = f"""
                WITH EAN_LOOKUP AS (
                    SELECT 
                        RTRIM(cProducto) AS cProducto,
                        RTRIM(cProductoEquiv) AS EAN
                    FROM dbo.maeGC_ProductoEquiv WITH (NOLOCK)
                    WHERE cEquivalencia = 'EAN12' 
                        AND cPais = '{codigo_pais}'
                )
                SELECT 
                    d.*,
                    COALESCE(e.EAN, '') AS EAN
                FROM {tabla} d WITH (NOLOCK, INDEX(0))
                LEFT JOIN EAN_LOOKUP e ON RTRIM(d.cProductoVta) = e.cProducto
                OPTION (MAXDOP 4)
                """
        elif columna_fecha:
            fecha_inicio = (datetime.now() - timedelta(days=36*30)).strftime('%Y-%m-%d')
            query = f"""
            SELECT * 
            FROM {tabla} WITH (NOLOCK, INDEX(0))
            WHERE {columna_fecha} >= '{fecha_inicio}'
            OPTION (MAXDOP 4, OPTIMIZE FOR UNKNOWN)
            """
        else:
            query = f"""
            SELECT * FROM {tabla} WITH (NOLOCK, INDEX(0))
            OPTION (MAXDOP 4)
            """
        
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        
        chunk_size = 5000
        all_rows = []
        while True:
            chunk = cursor.fetchmany(chunk_size)
            if not chunk:
                break
            all_rows.extend(chunk)
        
        if all_rows and columns:
            df = pd.DataFrame.from_records(all_rows, columns=columns)
            
            # Desambiguar columnas duplicadas
            df = desambiguar_columnas(df)
            
            if tabla.upper() == 'MOVGC_VTDOCUMENTOVTADET' and 'EAN' in df.columns:
                ean_count = (df['EAN'] != '').sum()
                st.success(f"✅ {pais}: Columna EAN incluida - {ean_count:,} de {len(df):,} ({ean_count/len(df)*100:.1f}%)")
            
            return df
        return pd.DataFrame()
        
    except Exception as e:
        st.error(f"❌ {pais}: Error descargando tabla {tabla} - {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def guardar_csv(df: pd.DataFrame, pais: str, nombre: str) -> str:
    """Guarda CSV en carpeta de Google Drive"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{nombre.replace(' ', '_')}_{timestamp}.csv"
    filepath = BASE_DIR / pais / filename
    
    df.to_csv(filepath, index=False, encoding='utf-8-sig')
    return str(filepath)


def mover_archivos_a_back(pais: str) -> int:
    """Mueve archivos CSV existentes a carpeta back antes de nueva descarga"""
    carpeta_pais = BASE_DIR / pais
    carpeta_back = carpeta_pais / "back"
    
    # Crear carpeta back si no existe
    carpeta_back.mkdir(parents=True, exist_ok=True)
    
    # Buscar archivos CSV en carpeta principal (no en subcarpetas)
    archivos_csv = list(carpeta_pais.glob("*.csv"))
    
    archivos_movidos = 0
    timestamp_back = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    for archivo in archivos_csv:
        # Agregar timestamp al nombre para evitar sobrescritura
        nombre_base = archivo.stem
        extension = archivo.suffix
        nuevo_nombre = f"{nombre_base}_bak_{timestamp_back}{extension}"
        destino = carpeta_back / nuevo_nombre
        
        try:
            archivo.rename(destino)
            archivos_movidos += 1
        except Exception as e:
            st.warning(f"⚠️ {pais}: No se pudo mover {archivo.name}: {e}")
    
    # Verificar que la carpeta principal quedó vacía de CSVs
    archivos_restantes = list(carpeta_pais.glob("*.csv"))
    if archivos_restantes:
        st.warning(f"⚠️ {pais}: {len(archivos_restantes)} archivo(s) no pudieron moverse")
    else:
        st.info(f"✅ {pais}: Carpeta principal vacía y lista para nueva descarga")
    
    return archivos_movidos


# ==========================================================================
# INTERFAZ
# ==========================================================================

st.title("📥 Descarga SQL Server → Google Drive")
st.markdown(f"**Destino:** `{DRIVE_BASE_DIR}`")
st.markdown("---")

# Sidebar
with st.sidebar:
    st.header("⚙️ Configuración")
    
    paises_sel = st.multiselect(
        "🌎 Países",
        options=PAISES,
        default=[PAISES[0]],
        help="Selecciona uno o más países"
    )
    
    if not paises_sel:
        st.warning("⚠️ Selecciona al menos un país")
        st.stop()
    
    st.markdown("---")
    
    tipo_descarga = st.radio(
        "📥 Tipo de Descarga",
        options=["🔹 Reportes (SP)", "📊 Tablas Base (11)"],
        help="Reportes: SPs específicos\nTablas: 11 tablas base para ETL"
    )
    
    st.markdown("---")
    
    if tipo_descarga == "🔹 Reportes (SP)":
        reporte_sel = st.selectbox(
            "📋 Reporte",
            options=list(SPS_DISPONIBLES.keys())
        )
        
        config = SPS_DISPONIBLES[reporte_sel]
        st.info(config['desc'])
        
        with st.expander("🔧 Detalles"):
            st.code(f"SP: {config['sp']}")
            if config.get('agregar_ean'):
                st.success("✅ Incluye columna EAN automática")
    else:
        st.info("📊 Descarga de 11 tablas base para ETL")
        
        with st.expander("📋 Tablas (11)"):
            for tabla in TABLAS_BASE:
                if tabla == "movGC_vtDocumentoVtaDet":
                    st.text(f"✅ {tabla} (con EAN)")
                else:
                    st.text(f"• {tabla}")
        
        st.warning("⚠️ Esta descarga puede tardar varios minutos")

# Área principal
if tipo_descarga == "🔹 Reportes (SP)":
    st.header(reporte_sel)
    
    params = []
    if 'fecha_inicio' in config['params']:
        col1, col2 = st.columns(2)
        with col1:
            fecha_ini = st.date_input(
                "📅 Fecha Inicio",
                value=datetime.now().replace(day=1)
            )
            params.append(fecha_ini.strftime('%Y-%m-%d'))
        with col2:
            fecha_fin = st.date_input(
                "📅 Fecha Fin",
                value=datetime.now()
            )
            params.append(fecha_fin.strftime('%Y-%m-%d'))
    
    st.markdown("---")
    boton_texto = "▶️ Ejecutar Reporte"
else:
    st.header("📊 Descarga de Tablas Base")
    st.markdown("**Filtro:** Últimos 36 meses (donde aplique)")
    st.markdown("**EAN:** Incluido automáticamente en `movGC_vtDocumentoVtaDet`")
    st.markdown("---")
    boton_texto = "▶️ Descargar 11 Tablas"

# Ejecución
if st.button(boton_texto, type="primary", use_container_width=True):
    # Mover archivos existentes a back
    st.subheader("📦 Preparando Descarga")
    with st.expander("🗂️ Respaldo de archivos existentes", expanded=False):
        total_movidos = 0
        for pais in paises_sel:
            movidos = mover_archivos_a_back(pais)
            if movidos > 0:
                st.success(f"✅ {pais}: {movidos} archivo(s) movido(s) a /back")
                total_movidos += movidos
            else:
                st.info(f"ℹ️ {pais}: Sin archivos previos")
        
        if total_movidos > 0:
            st.success(f"✅ Total: {total_movidos} archivo(s) respaldado(s)")
    
    st.markdown("---")
    st.subheader("📊 Resultados")
    
    resultados = []
    
    if tipo_descarga == "🔹 Reportes (SP)":
        # MODO REPORTES
        progress = st.progress(0)
        status = st.empty()
        
        for idx, pais in enumerate(paises_sel):
            status.text(f"Procesando {pais}...")
            
            agregar_ean = config.get('agregar_ean', False)
            df = ejecutar_sp(pais, config['sp'], params if params else None, agregar_ean=agregar_ean)
            
            if df is not None and not df.empty:
                filepath = guardar_csv(df, pais, reporte_sel)
                
                resultados.append({
                    'País': pais,
                    'Tipo': 'Reporte',
                    'Nombre': reporte_sel,
                    'Registros': len(df),
                    'Columnas': len(df.columns),
                    'Archivo': Path(filepath).name
                })
                
                with st.expander(f"🌎 {pais} - {len(df):,} registros", expanded=False):
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("📊 Registros", f"{len(df):,}")
                    with col2:
                        st.metric("📋 Columnas", len(df.columns))
                    with col3:
                        st.metric("💾 Tamaño", f"{df.memory_usage(deep=True).sum() / 1024:.1f} KB")
                    
                    st.dataframe(df.head(50), use_container_width=True)
                    st.success(f"✅ `{filepath}`")
            
            elif df is not None:
                st.warning(f"⚠️ {pais}: Sin datos")
            
            progress.progress((idx + 1) / len(paises_sel))
        
        status.text("✅ Completado")
    
    else:
        # MODO TABLAS BASE
        total_ops = len(paises_sel) * len(TABLAS_BASE)
        progress = st.progress(0)
        status = st.empty()
        op_actual = 0
        
        for pais in paises_sel:
            st.markdown(f"### 🌎 {pais}")
            
            for tabla in TABLAS_BASE:
                op_actual += 1
                status.text(f"Descargando {tabla} de {pais}... ({op_actual}/{total_ops})")
                
                df = descargar_tabla(pais, tabla)
                
                if df is not None and not df.empty:
                    filepath = guardar_csv(df, pais, tabla)
                    
                    resultados.append({
                        'País': pais,
                        'Tipo': 'Tabla',
                        'Nombre': tabla,
                        'Registros': len(df),
                        'Columnas': len(df.columns),
                        'Archivo': Path(filepath).name
                    })
                    
                    st.success(f"✅ {tabla}: {len(df):,} registros, {len(df.columns)} columnas")
                
                elif df is not None:
                    st.warning(f"⚠️ {tabla}: Sin datos")
                else:
                    st.error(f"❌ {tabla}: Error")
                
                progress.progress(op_actual / total_ops)
        
        status.text("✅ Descarga completada")
    
    # Resumen
    if resultados:
        st.markdown("---")
        st.subheader("📁 Resumen de Descarga")
        
        df_res = pd.DataFrame(resultados)
        st.dataframe(df_res, use_container_width=True, hide_index=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("🌎 Países", len(paises_sel))
        with col2:
            st.metric("📊 Total Registros", f"{df_res['Registros'].sum():,}")
        with col3:
            st.metric("📁 Archivos", len(resultados))
        
        st.info(f"💡 **Próximos pasos:** Ejecuta `python pipeline_maestro.py` para procesar y cargar a Snowflake")
    else:
        st.warning("⚠️ No se obtuvieron resultados")

# Footer
st.markdown("---")
st.caption(f"Pipeline ETL - Paso 1: Descarga SQL Server | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
