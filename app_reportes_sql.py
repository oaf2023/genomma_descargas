"""
Aplicación Streamlit para ejecutar Stored Procedures de SQL Server
Soporta múltiples países: Chile, Colombia, Ecuador, Perú

Autor: Sistema
Fecha: 2025-12-10
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os
from typing import Optional, Dict, List
import io
import sys
from pathlib import Path

# Importación condicional de pyodbc (solo si está disponible)
PYODBC_DISPONIBLE = False
try:
    import pyodbc
    PYODBC_DISPONIBLE = True
except ImportError:
    # pyodbc no disponible - las funciones SQL Server mostrarán error apropiado
    pass

# Agregar directorio hashados al path
hashados_path = Path(__file__).parent.parent / "hashados"
if hashados_path.exists():
    sys.path.insert(0, str(hashados_path))

# Importar metadatos de tablas (diferencias entre países)
try:
    from tabla_metadata import (
        obtener_columna_fecha_filtro,
        necesita_join_para_fecha,
        get_tabla_metadata,
        CAMPOS_FALTANTES_POR_PAIS,
        CAMPOS_EXTRA_POR_PAIS,
        ESTADISTICAS_ANALISIS
    )
    METADATA_DISPONIBLE = True
except ImportError:
    METADATA_DISPONIBLE = False

# Importar sistema de hashing
HASHING_DISPONIBLE = False
try:
    from hash_control import HashControlador
    from utils import obtener_estadisticas_control, leer_historial_completo
    HASHING_DISPONIBLE = True
except ImportError as e:
    # No mostrar warning aquí, se mostrará en la interfaz si es necesario
    pass

# Configuración de la página SOLO si se ejecuta directamente
if __name__ == "__main__":
    st.set_page_config(
        page_title="Reportes SQL Server",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )

# ============================================================================
# CONFIGURACIÓN DE SERVIDORES POR PAÍS
# ============================================================================

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

DRIVER = 'ODBC Driver 18 for SQL Server'  # Actualizado a Driver 18 (funciona con TrustServerCertificate)

# ============================================================================
# CONFIGURACIÓN DE DIRECTORIOS - MULTIPLATAFORMA
# ============================================================================

def detectar_directorio_base():
    """
    Detecta y configura el directorio base según el sistema operativo
    
    WINDOWS:
    - Prioridad 1: Variable de entorno GENOMMA_BASE_DIR
    - Prioridad 2: Google Drive Desktop (G:\Mi unidad\ETL_Snowflake)
    - Prioridad 3: Carpeta local (C:\Ciencia de Datos\otros_datos)
    
    LINUX/CODESPACES:
    - Prioridad 1: Variable de entorno GENOMMA_BASE_DIR
    - Prioridad 2: Carpeta temporal (/tmp/genomma_reportes)
    """
    # Variable de entorno tiene máxima prioridad
    env_dir = os.getenv("GENOMMA_BASE_DIR")
    if env_dir and os.path.exists(os.path.dirname(env_dir)):
        return env_dir
    
    if os.name == 'nt':  # Windows
        # Intentar Google Drive Desktop primero
        google_drive = r'G:\Mi unidad\ETL_Snowflake'
        if os.path.exists(google_drive):
            return google_drive
        
        # Fallback a carpeta local Windows
        local_win = r'C:\Ciencia de Datos\otros_datos'
        return local_win
    else:  # Linux/Mac (Codespaces)
        return '/tmp/genomma_reportes'

# Directorio base para resultados
BASE_DIR = detectar_directorio_base()

# Crear carpetas por país si no existen
def crear_carpetas_paises():
    """Crea las carpetas para cada país si no existen"""
    global BASE_DIR
    try:
        # Crear directorio base si no existe
        os.makedirs(BASE_DIR, exist_ok=True)
        
        # Crear subdirectorios por país
        for pais in SERVERS_CONFIG.keys():
            pais_dir = os.path.join(BASE_DIR, pais)
            os.makedirs(pais_dir, exist_ok=True)
            
    except Exception as e:
        # Si falla, usar directorio temporal
        import tempfile
        BASE_DIR = tempfile.mkdtemp(prefix='genomma_reportes_')
        for pais in SERVERS_CONFIG.keys():
            pais_dir = os.path.join(BASE_DIR, pais)
            os.makedirs(pais_dir, exist_ok=True)

# Inicializar carpetas al cargar
crear_carpetas_paises()

# ============================================================================
# FUNCIONES DE CONEXIÓN Y EJECUCIÓN
# ============================================================================

def get_connection(pais: str):
    """Establece conexión con SQL Server para el país especificado
    
    IMPORTANTE: NO usa cache (@st.cache_resource) según estándar Sección 7 AGENTS.MD
    Cada llamada crea nueva conexión para evitar errores "Connection is busy"
    """
    if not PYODBC_DISPONIBLE:
        st.error("❌ pyodbc no está instalado. Las funciones de SQL Server no están disponibles en este entorno.")
        st.info("💡 Para usar esta funcionalidad, instala pyodbc y los drivers ODBC de SQL Server.")
        return None
        
    try:
        config = SERVERS_CONFIG[pais]
        conn_str = (
            f'DRIVER={{{DRIVER}}};'
            f'SERVER={config["server"]};'
            f'DATABASE={config["database"]};'
            f'UID={config["user"]};'
            f'PWD={config["password"]};'
            f'TrustServerCertificate=yes;'
            f'Timeout=300;'  # 5 minutos timeout para consultas grandes
        )
        conn = pyodbc.connect(conn_str, timeout=30)  # 30 seg timeout de conexión
        return conn
    except Exception as e:
        st.error(f"❌ Error conectando a {pais}: {str(e)}")
        return None


def ejecutar_sp(pais: str, sp_name: str, params: List = None) -> Optional[pd.DataFrame]:
    """Ejecuta un stored procedure y retorna los resultados como DataFrame"""
    cursor = None
    try:
        conn = get_connection(pais)
        if conn is None:
            return None
        
        cursor = conn.cursor()
        
        # Construir query
        if params:
            placeholders = ', '.join(['?' for _ in params])
            query = f"EXEC {sp_name} {placeholders}"
            cursor.execute(query, params)
        else:
            query = f"EXEC {sp_name}"
            cursor.execute(query)
        
        # Obtener columnas y datos usando fetchmany() - ESTÁNDAR SECCIÓN 7 AGENTS.MD
        columns = [column[0] for column in cursor.description] if cursor.description else []
        
        # Leer en chunks para reducir memoria 70-80%
        chunk_size = 10000  # Balance entre memoria y velocidad
        all_rows = []
        while True:
            chunk = cursor.fetchmany(chunk_size)
            if not chunk:
                break
            all_rows.extend(chunk)
        
        if all_rows and columns:
            df = pd.DataFrame.from_records(all_rows, columns=columns)
            # Normalizar nombres de columnas para evitar duplicados
            df = normalizar_columnas_duplicadas(df)
            
            # ENRIQUECER CON EAN si es reporte de ventas y no tiene columna EAN
            if 'ventas' in sp_name.lower() and 'EAN' not in df.columns:
                df = agregar_columna_ean(df, pais)
            
            return df
        else:
            return pd.DataFrame()
            
    except pyodbc.ProgrammingError as e:
        # SP no existe
        error_msg = str(e).lower()
        if "could not find stored procedure" in error_msg or "no se pudo encontrar" in error_msg:
            st.warning(f"⚠️ {pais}: El stored procedure '{sp_name}' no existe en este servidor")
            return None
        else:
            st.error(f"❌ Error de programación en {pais}: {str(e)}")
            return None
    except Exception as e:
        st.error(f"❌ Error ejecutando {sp_name} en {pais}: {str(e)}")
        return None
    finally:
        # IMPORTANTE: Cerrar cursor siempre para liberar conexión
        if cursor is not None:
            try:
                cursor.close()
            except:
                pass
        # Cerrar conexión explícitamente
        if conn is not None:
            try:
                conn.close()
            except:
                pass


def agregar_columna_ean(df: pd.DataFrame, pais: str) -> pd.DataFrame:
    """Agrega columna EAN al DataFrame mediante JOIN directo a maeGC_ProductoEquiv
    
    NOTA: Crea su PROPIA conexión para evitar conflictos de "Connection is busy".
    NO reutiliza la conexión del SP según AGENTS.MD sección 7.3.
    
    Busca columnas que puedan contener código de producto:
    - Código de producto
    - CodigoProducto  
    - cProducto
    - cProductoVta
    - ITEMNMBR
    """
    if df.empty:
        return df
    
    # Buscar columna de código de producto
    col_producto = None
    posibles_nombres = ['Código de producto', 'CodigoProducto', 'cProducto', 'cProductoVta', 
                        'ITEMNMBR', 'Codigo Producto', 'Codigo de producto']
    
    for nombre in posibles_nombres:
        if nombre in df.columns:
            col_producto = nombre
            break
    
    if not col_producto:
        # No hay columna de producto, devolver sin cambios
        df['EAN'] = ''
        st.warning(f"⚠️ {pais}: No se encontró columna de código de producto para agregar EAN")
        return df
    
    # ========================================================================
    # JOIN directo a maeGC_ProductoEquiv en SQL Server
    # ========================================================================
    conn = None
    cursor = None
    try:
        # CREAR NUEVA CONEXIÓN (no reusar) - AGENTS.MD sección 7.3
        conn = get_connection(pais)
        if conn is None:
            df['EAN'] = ''
            st.warning(f"⚠️ {pais}: No se pudo conectar para obtener códigos EAN")
            return df
        
        # Mapeo de códigos de país
        codigo_pais = {'CHILE': 'CL', 'PERU': 'PE', 'COLOMBIA': 'CO', 'ECUADOR': 'EC'}.get(pais, 'CL')
        
        # Query para obtener mapeo EAN
        cursor = conn.cursor()
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
        columns_ean = [column[0] for column in cursor.description]
        rows_ean = cursor.fetchall()
        
        if rows_ean and columns_ean:
            # Crear DataFrame de mapeo
            df_ean = pd.DataFrame.from_records(rows_ean, columns=columns_ean)
            
            # Normalizar códigos de producto (quitar espacios)
            df['_codigo_limpio'] = df[col_producto].astype(str).str.strip()
            df_ean['cProducto'] = df_ean['cProducto'].astype(str).str.strip()
            
            # Hacer merge
            df = df.merge(
                df_ean[['cProducto', 'EAN']],
                left_on='_codigo_limpio',
                right_on='cProducto',
                how='left'
            )
            
            # Limpiar columnas temporales
            df.drop(columns=['_codigo_limpio', 'cProducto'], inplace=True, errors='ignore')
            
            # Rellenar vacíos
            df['EAN'] = df['EAN'].fillna('').astype(str)
            
            # Contar EANs agregados
            ean_count = (df['EAN'] != '').sum()
            st.success(f"✅ {pais}: {ean_count:,} códigos EAN desde maeGC_ProductoEquiv ({ean_count/len(df)*100:.1f}%)")
        else:
            df['EAN'] = ''
            st.warning(f"⚠️ {pais}: No se encontraron códigos EAN en maeGC_ProductoEquiv")
            
    except Exception as e:
        st.error(f"❌ {pais}: Error al agregar columna EAN: {str(e)}")
        df['EAN'] = ''
    finally:
        # Cerrar cursor y conexión explícitamente - AGENTS.MD sección 7.3
        if cursor is not None:
            try:
                cursor.close()
            except:
                pass
        if conn is not None:
            try:
                conn.close()
            except:
                pass
    
    return df


def verificar_sp_existe(pais: str, sp_name: str) -> bool:
    """Verifica si un stored procedure existe en el servidor"""
    try:
        conn = get_connection(pais)
        if conn is None:
            return False
        
        cursor = conn.cursor()
        
        # Query para verificar existencia del SP
        query = """
        SELECT COUNT(*) 
        FROM sys.objects 
        WHERE type = 'P' 
        AND name = ?
        """
        cursor.execute(query, sp_name)
        result = cursor.fetchone()
        cursor.close()
        
        return result[0] > 0
        
    except Exception as e:
        st.error(f"❌ Error verificando SP en {pais}: {str(e)}")
        return False


# ============================================================================
# FUNCIONES DE DESCARGA DE TABLAS
# ============================================================================

def normalizar_nombre_archivo(nombre: str) -> str:
    """Normaliza nombre de archivo para evitar caracteres problemáticos
    
    Convierte acentos y caracteres especiales (á→a, ñ→n, etc.) a ASCII.
    Reemplaza espacios y símbolos por guiones bajos.
    Elimina caracteres no permitidos en nombres de archivo.
    
    Ejemplos:
        'Año_Fiscal' → 'Ano_Fiscal'
        'Región-País' → 'Region_Pais'
        'Venta/Día.csv' → 'Venta_Dia_csv'
    """
    import re
    try:
        from unidecode import unidecode
        # Transliterar caracteres unicode a ASCII (á→a, ñ→n, ü→u, etc.)
        nombre = unidecode(nombre)
    except ImportError:
        # Fallback: reemplazo manual de caracteres comunes del español
        reemplazos = {
            'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
            'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U',
            'ñ': 'n', 'Ñ': 'N',
            'ü': 'u', 'Ü': 'U',
            '¿': '', '¡': '',
            'ç': 'c', 'Ç': 'C'
        }
        for orig, repl in reemplazos.items():
            nombre = nombre.replace(orig, repl)
    
    # Reemplazar caracteres especiales comunes por guiones bajos
    nombre = nombre.replace(' ', '_')
    nombre = nombre.replace('-', '_')
    nombre = nombre.replace('.', '_')
    nombre = nombre.replace('/', '_')
    nombre = nombre.replace('\\', '_')
    nombre = nombre.replace(':', '_')
    nombre = nombre.replace('*', '_')
    nombre = nombre.replace('?', '_')
    nombre = nombre.replace('"', '_')
    nombre = nombre.replace('<', '_')
    nombre = nombre.replace('>', '_')
    nombre = nombre.replace('|', '_')
    nombre = nombre.replace('(', '_')
    nombre = nombre.replace(')', '_')
    nombre = nombre.replace('[', '_')
    nombre = nombre.replace(']', '_')
    nombre = nombre.replace('{', '_')
    nombre = nombre.replace('}', '_')
    nombre = nombre.replace(',', '_')
    nombre = nombre.replace(';', '_')
    nombre = nombre.replace('=', '_')
    nombre = nombre.replace('+', '_')
    nombre = nombre.replace('&', '_')
    nombre = nombre.replace('%', '_')
    nombre = nombre.replace('$', '_')
    nombre = nombre.replace('#', '_')
    nombre = nombre.replace('@', '_')
    nombre = nombre.replace('!', '_')
    
    # Eliminar caracteres no alfanuméricos (solo ASCII) excepto guión bajo
    nombre = re.sub(r'[^a-zA-Z0-9_]', '', nombre)
    
    # Eliminar guiones bajos múltiples consecutivos
    nombre = re.sub(r'_+', '_', nombre)
    
    # Eliminar guiones bajos al inicio y final
    nombre = nombre.strip('_')
    
    # ✅ NORMA SNOWFLAKE: Todo en MAYÚSCULAS
    nombre = nombre.upper()
    
    # ✅ NORMA SNOWFLAKE: Máximo 128 caracteres
    if len(nombre) > 128:
        nombre = nombre[:128].rstrip('_')
    
    return nombre


def leer_tablas_a_descargar() -> List[str]:
    """Lee el archivo CSV con las tablas a descargar"""
    try:
        # Buscar el CSV en el directorio del script (repositorio)
        script_dir = Path(__file__).parent
        csv_path = script_dir / 'tablas_a_descargar.csv'
        
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            if 'nombre_tabla' not in df.columns:
                st.error(f"❌ El archivo CSV debe tener una columna 'nombre_tabla'")
                return []
            return df['nombre_tabla'].tolist()
        else:
            st.error(f"❌ No se encontró el archivo tablas_a_descargar.csv en {script_dir}")
            st.info(f"💡 Busca el archivo en: `{csv_path}`")
            return []
    except Exception as e:
        st.error(f"❌ Error leyendo tablas_a_descargar.csv: {str(e)}")
        return []


def detectar_columna_fecha(pais: str, tabla: str) -> Optional[str]:
    """Detecta la columna de fecha para filtro de 36 meses
    
    Usa metadatos predefinidos si están disponibles, sino hace query a INFORMATION_SCHEMA.
    """
    # Intentar usar metadatos predefinidos
    if METADATA_DISPONIBLE:
        columna_metadata = obtener_columna_fecha_filtro(tabla)
        if columna_metadata:
            return columna_metadata
    
    # Fallback: Query dinámica a INFORMATION_SCHEMA
    cursor = None
    try:
        conn = get_connection(pais)
        if conn is None:
            return None
        
        cursor = conn.cursor()
        
        # Query para obtener columnas de tipo fecha
        query = f"""
        SELECT TOP 1 COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{tabla}'
        AND DATA_TYPE IN ('datetime', 'date', 'smalldatetime', 'datetime2')
        ORDER BY ORDINAL_POSITION
        """
        cursor.execute(query)
        result = cursor.fetchone()
        
        if result:
            return result[0]
        return None
        
    except:
        return None
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except:
                pass


def descargar_tabla(pais: str, tabla: str) -> Optional[pd.DataFrame]:
    """Descarga una tabla completa o filtrada por últimos 36 meses
    
    Maneja diferencias de campos entre países automáticamente.
    OPCIÓN 1: Para tablas de facturación, incluye columna EAN con JOIN a ProductoEquiv.
    """
    cursor = None
    try:
        conn = get_connection(pais)
        if conn is None:
            return None
        
        # Obtener metadatos si están disponibles
        campos_faltantes = []
        campos_extra = []
        if METADATA_DISPONIBLE:
            if pais in CAMPOS_FALTANTES_POR_PAIS and tabla in CAMPOS_FALTANTES_POR_PAIS[pais]:
                campos_faltantes = CAMPOS_FALTANTES_POR_PAIS[pais][tabla]
            if pais in CAMPOS_EXTRA_POR_PAIS and tabla in CAMPOS_EXTRA_POR_PAIS[pais]:
                campos_extra = CAMPOS_EXTRA_POR_PAIS[pais][tabla]
        
        # Detectar si hay columna de fecha
        columna_fecha = detectar_columna_fecha(pais, tabla)
        
        cursor = conn.cursor()
        
        # ESTÁNDAR SECCIÓN 7 AGENTS.MD: Optimizaciones SQL Server
        # SET NOCOUNT ON: Reduce tráfico de red
        cursor.execute("SET NOCOUNT ON")
        cursor.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        
        # ====================================================================
        # OPCIÓN 1: QUERY ESPECIAL PARA TABLAS DE FACTURACIÓN CON EAN
        # ====================================================================
        # Mapeo de códigos de país para filtro en ProductoEquiv
        codigo_pais = {'CHILE': 'CL', 'PERU': 'PE', 'COLOMBIA': 'CO', 'ECUADOR': 'EC'}.get(pais, 'CL')
        
        # Si es tabla de facturación DETALLE (tiene productos), incluir EAN con JOIN
        # NOTA: movGC_VtDocumentoVtaCab es cabecera (sin productos), solo Det tiene cProductoVta
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
        # Query normal para otras tablas
        elif columna_fecha:
            # Descargar solo últimos 36 meses
            fecha_inicio = (datetime.now() - timedelta(days=36*30)).strftime('%Y-%m-%d')
            query = f"""
            SELECT * 
            FROM {tabla} WITH (NOLOCK, INDEX(0))
            WHERE {columna_fecha} >= '{fecha_inicio}'
            OPTION (MAXDOP 4, OPTIMIZE FOR UNKNOWN)
            """
        else:
            # Descargar tabla completa (sin filtro de fecha)
            query = f"""
            SELECT * FROM {tabla} WITH (NOLOCK, INDEX(0))
            OPTION (MAXDOP 4)
            """
        
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        
        # Usar fetchmany() - Reduce memoria 70-80%
        chunk_size = 5000  # Para datasets grandes
        all_rows = []
        while True:
            chunk = cursor.fetchmany(chunk_size)
            if not chunk:
                break
            all_rows.extend(chunk)
        
        rows = all_rows
        
        if rows and columns:
            df = pd.DataFrame.from_records(rows, columns=columns)
            
            # Normalizar columnas duplicadas (solo NOMBRES, NO datos)
            # Agrega sufijos _1, _2 a columnas con nombres duplicados
            df = normalizar_columnas_duplicadas(df)
            
            # Log informativo si se incluyó columna EAN (solo para tabla de DETALLE)
            if tabla.upper() == 'MOVGC_VTDOCUMENTOVTADET':
                if 'EAN' in df.columns:
                    ean_count = df['EAN'].notna().sum()
                    ean_no_vacio = (df['EAN'] != '').sum()
                    st.success(f"✅ Columna EAN incluida: {ean_no_vacio:,} registros con EAN de {len(df):,} totales ({ean_no_vacio/len(df)*100:.1f}%)")
                else:
                    st.warning(f"⚠️ Tabla {tabla} descargada SIN columna EAN")
            
            # Información sobre diferencias (solo para debug)
            if campos_faltantes:
                # Silencioso - campos faltantes son esperados
                pass
            if campos_extra:
                # Silencioso - campos extra están incluidos en SELECT *
                pass
            
            return df
        else:
            return pd.DataFrame()
        
    except Exception as e:
        st.error(f"❌ Error descargando tabla {tabla} en {pais}: {str(e)}")
        return None
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except:
                pass
        # Cerrar conexión explícitamente - NO usar cache
        if conn is not None:
            try:
                conn.close()
            except:
                pass


def descargar_todas_las_tablas(paises: List[str]):
    """Descarga todas las tablas del CSV para los países seleccionados
    
    Usa metadatos de diferencias entre países para manejo robusto.
    """
    # Verificar disponibilidad de pyodbc
    if not PYODBC_DISPONIBLE:
        st.error("❌ **pyodbc no está instalado**")
        st.warning("⚠️ Las funciones de SQL Server no están disponibles en este entorno.")
        st.info("💡 **Para usar esta funcionalidad:**\n"
                "- Instala pyodbc: `pip install pyodbc`\n"
                "- Instala los drivers ODBC de SQL Server\n"
                "- En Windows: ODBC Driver 18 for SQL Server\n"
                "- En Linux: unixODBC + msodbcsql18")
        return
    
    tablas = leer_tablas_a_descargar()
    
    if not tablas:
        st.warning("⚠️ No hay tablas para descargar")
        return
    
    # Mostrar info de inicio
    st.info(f"🚀 **Iniciando descarga de {len(tablas)} tabla(s) para {len(paises)} país(es)**")
    
    # Mostrar directorio de destino
    if os.name == 'nt':  # Windows
        if 'Google Drive' in BASE_DIR or 'Mi unidad' in BASE_DIR:
            st.success(f"📂 **Guardando en Google Drive:** `{BASE_DIR}`")
        else:
            st.info(f"📂 **Guardando en:** `{BASE_DIR}`")
    else:  # Linux/Codespaces
        st.warning(f"⚠️ **Guardando en directorio temporal:** `{BASE_DIR}`")
        st.caption("Los archivos se eliminarán al cerrar la sesión. Descárgalos después.")
    
    # Mostrar info de metadatos si está disponible
    if METADATA_DISPONIBLE:
        st.info(f"ℹ️ Sistema de metadatos activo - Manejo automático de diferencias entre países")
    
    total_operaciones = len(paises) * len(tablas)
    operacion_actual = 0
    
    st.subheader(f"📥 Descargando {len(tablas)} tablas de {len(paises)} país(es)")
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    resumen = []
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Bandera para mostrar mensaje de conexión solo una vez
    primera_conexion = True
    
    for pais in paises:
        st.markdown(f"### 🌎 {pais}")
        
        # Mostrar mensaje de conexión en el primer país
        if primera_conexion:
            with st.spinner(f"🔌 Conectando a SQL Server ({pais})..."):
                # Probar conexión
                test_conn = get_connection(pais)
                if test_conn is None:
                    st.error(f"❌ **No se pudo conectar a SQL Server para {pais}**")
                    st.info("💡 Verifica:\n"
                           "- Drivers ODBC instalados\n"
                           "- Conectividad de red\n"
                           "- Credenciales correctas")
                    return
                else:
                    st.success(f"✅ **Conexión establecida con {pais}**")
                    test_conn.close()
                    primera_conexion = False
        
        for tabla in tablas:
            operacion_actual += 1
            status_text.text(f"Descargando {tabla} de {pais}... ({operacion_actual}/{total_operaciones})")
            
            df = descargar_tabla(pais, tabla)
            
            if df is not None and not df.empty:
                # ============================================================
                # VERIFICACIÓN DE INTEGRIDAD CON HASHING
                # ============================================================
                hash_resultado = None
                if HASHING_DISPONIBLE:
                    try:
                        controlador = HashControlador()
                        
                        # Detectar columnas clave y float automáticamente
                        columnas_clave = []
                        columnas_float = []
                        
                        for col in df.columns:
                            if 'fecha' in col.lower() or 'date' in col.lower():
                                columnas_clave.append(col)
                            elif df[col].dtype in ['float64', 'float32']:
                                columnas_float.append(col)
                        
                        # Si no hay columnas de fecha, usar primeras 2 columnas
                        if not columnas_clave:
                            columnas_clave = df.columns[:2].tolist()
                        
                        hash_resultado = controlador.procesar_tabla(
                            df=df,
                            pais=pais,
                            tabla=tabla,
                            columnas_clave=columnas_clave,
                            columnas_float=columnas_float if columnas_float else None
                        )
                        
                        # Mostrar resultado de verificación
                        if hash_resultado['verificacion']['resultado'] == 'MODIFICADO':
                            st.warning(f"⚠️ {tabla}: Modificación histórica detectada - v{hash_resultado['version']}")
                        elif hash_resultado['verificacion']['resultado'] == 'OK':
                            st.info(f"ℹ️ {tabla}: Sin modificaciones - v{hash_resultado['version']}")
                    
                    except Exception as e:
                        st.warning(f"⚠️ Error en verificación de hash para {tabla}: {str(e)}")
                
                # Guardar en carpeta del país
                try:
                    pais_dir = os.path.join(BASE_DIR, pais)
                    
                    # Normalizar SOLO el nombre del archivo (metadatos)
                    # NO toca los datos: solo convierte nombre tabla a formato Snowflake
                    tabla_normalizada = normalizar_nombre_archivo(tabla)
                    nombre_archivo = f"{tabla_normalizada}_{timestamp}.csv"
                    ruta_completa = os.path.join(pais_dir, nombre_archivo)
                    
                    df.to_csv(ruta_completa, index=False, encoding='utf-8-sig')
                    
                    # Agregar info de hash al resumen
                    estado_hash = ""
                    if hash_resultado:
                        if hash_resultado['verificacion']['resultado'] == 'MODIFICADO':
                            estado_hash = " ⚠️ MODIFICADO"
                        elif hash_resultado['verificacion']['resultado'] == 'OK':
                            estado_hash = " ✓ Hash OK"
                        elif hash_resultado['verificacion']['resultado'] == 'INICIAL':
                            estado_hash = " 🆕 Inicial"
                    
                    resumen.append({
                        'Pais': pais,
                        'Tabla': tabla,
                        'Registros': len(df),
                        'Columnas': len(df.columns),
                        'Tamaño (KB)': df.memory_usage(deep=True).sum() / 1024,
                        'Archivo': nombre_archivo,
                        'Hash': hash_resultado['hash_nuevo'][:16] + "..." if hash_resultado else "N/A",
                        'Versión': f"v{hash_resultado['version']}" if hash_resultado else "N/A",
                        'Estado': f'✅ OK{estado_hash}'
                    })
                    
                    st.success(f"✅ {tabla}: {len(df):,} registros{estado_hash}")
                    
                except Exception as e:
                    resumen.append({
                        'Pais': pais,
                        'Tabla': tabla,
                        'Registros': 0,
                        'Columnas': 0,
                        'Tamaño (KB)': 0,
                        'Archivo': '',
                        'Estado': f'❌ Error: {str(e)}'
                    })
                    st.error(f"❌ Error guardando {tabla}: {e}")
            elif df is not None:
                resumen.append({
                    'Pais': pais,
                    'Tabla': tabla,
                    'Registros': 0,
                    'Columnas': 0,
                    'Tamaño (KB)': 0,
                    'Archivo': '',
                    'Estado': '⚠️ Sin datos'
                })
                st.warning(f"⚠️ {tabla}: Sin datos")
            else:
                resumen.append({
                    'Pais': pais,
                    'Tabla': tabla,
                    'Registros': 0,
                    'Columnas': 0,
                    'Tamaño (KB)': 0,
                    'Archivo': '',
                    'Estado': '❌ Error'
                })
            
            progress_bar.progress(operacion_actual / total_operaciones)
    
    status_text.text("✅ Descarga completada")
    
    # Mostrar resumen
    st.markdown("---")
    st.subheader("📊 Resumen de Descarga")
    
    df_resumen = pd.DataFrame(resumen)
    st.dataframe(df_resumen, use_container_width=True, hide_index=True)
    
    # Métricas totales
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("🌎 Países", len(paises))
    with col2:
        st.metric("📋 Tablas", len(tablas))
    with col3:
        exitosos = len(df_resumen[df_resumen['Estado'].str.contains('✅', na=False)])
        st.metric("✅ Exitosos", f"{exitosos}/{len(resumen)}")
    with col4:
        total_registros = df_resumen['Registros'].sum()
        st.metric("📊 Total Registros", f"{total_registros:,}")
    
    # ========================================
    # BOTONES DE DESCARGA (para Codespaces)
    # ========================================
    en_codespaces = os.getenv('CODESPACES') == 'true' or '/workspaces/' in os.getcwd()
    
    if en_codespaces:
        st.markdown("---")
        st.info("💾 **Archivos listos en el servidor temporal**. Descárgalos a tu PC usando los botones abajo:")
        
        # Crear archivo ZIP con todos los CSVs
        import zipfile
        import io
        
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for pais in paises:
                pais_dir = os.path.join(BASE_DIR, pais)
                if os.path.exists(pais_dir):
                    for archivo in os.listdir(pais_dir):
                        if archivo.endswith(f'_{timestamp}.csv'):
                            ruta_archivo = os.path.join(pais_dir, archivo)
                            zip_file.write(ruta_archivo, f"{pais}/{archivo}")
        
        zip_buffer.seek(0)
        
        st.download_button(
            label="📦 Descargar TODOS los archivos (ZIP)",
            data=zip_buffer,
            file_name=f"genomma_tablas_{timestamp}.zip",
            mime="application/zip",
            use_container_width=True
        )
        
        st.caption(f"📁 Archivos temporales en: `{BASE_DIR}` (se borrarán al cerrar Codespaces)")
    else:
        st.success(f"✅ **Archivos guardados en:** `{BASE_DIR}`")


# ============================================================================
# FUNCIONES PYTHON ALTERNATIVAS (cuando no existe SP)
# ============================================================================

def normalizar_columnas_duplicadas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza nombres de columnas duplicadas agregando sufijos _1, _2, etc.
    
    ⚠️ IMPORTANTE: Esta función SOLO renombra COLUMNAS (metadatos).
                  NUNCA modifica los DATOS dentro del DataFrame.
    
    Los valores de las celdas permanecen completamente intactos.
    Solo cambia el encabezado de las columnas.
    
    Ejemplo:
        Entrada:  ['Nombre', 'Dirección', 'Dirección', 'Teléfono']
        Salida:   ['Nombre', 'Dirección', 'Dirección_1', 'Teléfono']
        Datos:    Sin cambios (valores preservados tal cual)
    """
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        dup_indices = cols[cols == dup].index
        for i, idx in enumerate(dup_indices[1:], start=1):
            cols.iloc[idx] = f"{dup}_{i}"
    df.columns = cols
    return df


def ejecutar_query_alternativa(pais: str, query: str) -> Optional[pd.DataFrame]:
    """Ejecuta una query SQL directa como alternativa a SP"""
    cursor = None
    try:
        conn = get_connection(pais)
        if conn is None:
            return None
        
        cursor = conn.cursor()
        
        # Optimizaciones SQL Server - ESTÁNDAR SECCIÓN 7
        cursor.execute("SET NOCOUNT ON")
        cursor.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        
        cursor.execute(query)
        
        # Obtener columnas y datos usando fetchmany()
        columns = [column[0] for column in cursor.description] if cursor.description else []
        
        chunk_size = 10000
        all_rows = []
        while True:
            chunk = cursor.fetchmany(chunk_size)
            if not chunk:
                break
            all_rows.extend(chunk)
        
        if all_rows and columns:
            df = pd.DataFrame.from_records(all_rows, columns=columns)
            return df
        else:
            return pd.DataFrame()
        
    except Exception as e:
        st.error(f"❌ Error ejecutando query alternativa en {pais}: {str(e)}")
        return None
    finally:
        # IMPORTANTE: Cerrar cursor siempre para liberar conexión
        if cursor is not None:
            try:
                cursor.close()
            except:
                pass
        # Cerrar conexión explícitamente
        if conn is not None:
            try:
                conn.close()
            except:
                pass


def reporte_ventas_alternativo(pais: str, fecha_inicio: str, fecha_fin: str) -> Optional[pd.DataFrame]:
    """Función alternativa para Reporte Único de Ventas con columna EAN"""
    # Mapeo de códigos de país
    codigo_pais = {'CHILE': 'CL', 'PERU': 'PE', 'COLOMBIA': 'CO', 'ECUADOR': 'EC'}.get(pais, 'CL')
    
    # Intentar primero con tablas movGC (si existen)
    query_movgc = f"""
    WITH EAN_LOOKUP AS (
        SELECT 
            RTRIM(cProducto) AS cProducto,
            RTRIM(cProductoEquiv) AS EAN
        FROM dbo.maeGC_ProductoEquiv WITH (NOLOCK)
        WHERE cEquivalencia = 'EAN12' 
            AND cPais = '{codigo_pais}'
    )
    SELECT 
        cab.cSerie + '-' + CAST(cab.nCorrelativo AS VARCHAR) AS NumeroDocumento,
        cab.fEmision AS FechaDocumento,
        cab.cCliente AS CodigoCliente,
        cab.dCliente AS NombreCliente,
        det.cProductoVta AS CodigoProducto,
        det.dProductoVta AS DescripcionProducto,
        COALESCE(ean.EAN, '') AS EAN,
        det.nCantidad AS Cantidad,
        det.nPrecioOrig AS PrecioUnitario,
        det.nTotalOrig AS PrecioExtendido,
        cab.nSubTotal AS Subtotal,
        cab.nImpuesto AS Impuesto,
        cab.nTotal AS TotalDocumento
    FROM dbo.movGC_VtDocumentoVtaCab cab WITH (NOLOCK, INDEX(0))
    INNER JOIN dbo.movGC_VtDocumentoVtaDet det WITH (NOLOCK, INDEX(0)) 
        ON cab.cSerie = det.cSerie AND cab.nCorrelativo = det.nCorrelativo
    LEFT JOIN EAN_LOOKUP ean ON RTRIM(det.cProductoVta) = ean.cProducto
    WHERE cab.fEmision >= '{fecha_inicio}' 
        AND cab.fEmision <= '{fecha_fin}'
    OPTION (MAXDOP 4, OPTIMIZE FOR UNKNOWN)
    """
    
    try:
        # Intentar query con tablas movGC
        df = ejecutar_query_alternativa(pais, query_movgc)
        if df is not None and not df.empty:
            return df
    except:
        pass
    
    # Fallback a tablas GP (sin EAN por ahora)
    query_gp = f"""
    SELECT 
        doc.SOPNUMBE AS NumeroDocumento,
        doc.DOCDATE AS FechaDocumento,
        doc.CUSTNMBR AS CodigoCliente,
        cust.CUSTNAME AS NombreCliente,
        det.ITEMNMBR AS CodigoProducto,
        det.ITEMDESC AS DescripcionProducto,
        '' AS EAN,
        det.QUANTITY AS Cantidad,
        det.UNITPRCE AS PrecioUnitario,
        det.XTNDPRCE AS PrecioExtendido,
        doc.SUBTOTAL AS Subtotal,
        doc.TAXAMNT AS Impuesto,
        doc.DOCAMNT AS TotalDocumento
    FROM SOP30200 doc WITH (NOLOCK)
    INNER JOIN SOP30300 det WITH (NOLOCK) ON doc.SOPNUMBE = det.SOPNUMBE AND doc.SOPTYPE = det.SOPTYPE
    LEFT JOIN RM00101 cust WITH (NOLOCK) ON doc.CUSTNMBR = cust.CUSTNMBR
    WHERE doc.DOCDATE BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    ORDER BY doc.DOCDATE, doc.SOPNUMBE
    """
    return ejecutar_query_alternativa(pais, query_gp)


def listar_clientes_alternativo(pais: str) -> Optional[pd.DataFrame]:
    """Función alternativa para Listar Clientes"""
    # Query base sin columnas que pueden no existir
    query = """
    SELECT 
        RTRIM(CUSTNMBR) AS [Cod. Cliente],
        RTRIM(CUSTNAME) AS [Razón],
        RTRIM(ADDRESS1) AS [Dirección],
        RTRIM(COUNTRY) AS [Pais],
        RTRIM(CITY) AS [Ciudad],
        RTRIM(STATE) AS [Estado],
        RTRIM(CUSTCLAS) AS [Crédito],
        RTRIM(CURNCYID) AS [Moneda],
        CRLMTAMT AS [Cupo],
        (CASE WHEN INACTIVE = 1 THEN 'SI' ELSE 'NO' END) AS [Inactivo],
        (CASE WHEN HOLD = 1 THEN 'SI' ELSE 'NO' END) AS [Suspendido],
        CREATDDT AS [Fec. Creación]
    FROM RM00101 WITH (NOLOCK)
    ORDER BY CUSTNAME
    """
    
    df = ejecutar_query_alternativa(pais, query)
    
    # Intentar agregar columna Correo si existe INET1
    if df is not None and not df.empty:
        try:
            # Intentar consulta silenciosa de email
            conn = get_connection(pais)
            if conn is not None:
                cursor = conn.cursor()
                try:
                    query_email = """
                    SELECT 
                        RTRIM(CUSTNMBR) AS [Cod. Cliente],
                        RTRIM(INET1) AS [Correo]
                    FROM RM00101 WITH (NOLOCK)
                    WHERE INET1 IS NOT NULL
                    """
                    cursor.execute(query_email)
                    columns = [column[0] for column in cursor.description]
                    rows = cursor.fetchall()
                    
                    if rows and columns:
                        df_email = pd.DataFrame.from_records(rows, columns=columns)
                        df = df.merge(df_email, on='Cod. Cliente', how='left')
                    else:
                        df['Correo'] = ''
                except:
                    # Columna INET1 no existe, agregar vacía silenciosamente
                    df['Correo'] = ''
                finally:
                    cursor.close()
            else:
                df['Correo'] = ''
        except:
            # Si falla por cualquier razón, agregar columna vacía
            df['Correo'] = ''
    
    return df


def listar_productos_alternativo(pais: str) -> Optional[pd.DataFrame]:
    """Función alternativa para Listar Productos"""
    # Query base con columnas que existen en todas las BD
    query = """
    SELECT 
        RTRIM(ITEMNMBR) AS [Código],
        RTRIM(ITEMDESC) AS [Descripción],
        RTRIM(ITMSHNAM) AS [Desc. Corta],
        RTRIM(ITMCLSCD) AS [Clase],
        RTRIM(ITEMTYPE) AS [Tipo],
        CURRCOST AS [Costo Actual],
        STNDCOST AS [Costo Estándar],
        (CASE WHEN INACTIVE = 1 THEN 'SI' ELSE 'NO' END) AS [Inactivo]
    FROM IV00101 WITH (NOLOCK)
    ORDER BY ITEMDESC
    """
    
    df = ejecutar_query_alternativa(pais, query)
    
    # Intentar agregar columnas adicionales si existen
    if df is not None and not df.empty:
        try:
            conn = get_connection(pais)
            if conn is not None:
                cursor = conn.cursor()
                try:
                    # Intentar obtener columnas de precio y cantidades
                    query_extra = """
                    SELECT 
                        RTRIM(ITEMNMBR) AS [Código],
                        LISTPRCE AS [Precio Lista],
                        QTYONHND AS [Cant. Disponible],
                        QTYSOLD AS [Cant. Vendida],
                        QTYONORD AS [Cant. Ordenada]
                    FROM IV00101 WITH (NOLOCK)
                    """
                    cursor.execute(query_extra)
                    columns = [column[0] for column in cursor.description]
                    rows = cursor.fetchall()
                    
                    if rows and columns:
                        df_extra = pd.DataFrame.from_records(rows, columns=columns)
                        df = df.merge(df_extra, on='Código', how='left')
                except:
                    # Las columnas no existen, continuar sin ellas
                    pass
                finally:
                    cursor.close()
        except:
            # Si falla, continuar con el DataFrame base
            pass
    
    return df


def reporte_cartera_alternativo(pais: str, fecha_inicio: str, fecha_fin: str) -> Optional[pd.DataFrame]:
    """Función alternativa para Reporte de Cartera"""
    query = f"""
    SELECT 
        RTRIM(rm.CUSTNMBR) AS NumeroCliente,
        RTRIM(cust.CUSTNAME) AS NombreCliente,
        RTRIM(rm.DOCNUMBR) AS NumeroDocumento,
        RTRIM(dt.DOCDESCR) AS TipoDocumento,
        rm.DOCDATE AS FechaDocumento,
        rm.DUEDATE AS FechaVencimiento,
        RTRIM(rm.CURNCYID) AS Moneda,
        rm.ORTRXAMT AS MontoOriginal,
        rm.CURTRXAM AS MontoActual,
        (rm.ORTRXAMT - rm.CURTRXAM) AS MontoPagado,
        DATEDIFF(day, rm.DUEDATE, GETDATE()) AS DiasVencido
    FROM RM20101 rm WITH (NOLOCK)
    INNER JOIN RM00101 cust WITH (NOLOCK) ON rm.CUSTNMBR = cust.CUSTNMBR
    LEFT JOIN RM40401 dt WITH (NOLOCK) ON rm.RMDTYPAL = dt.RMDTYPAL
    WHERE rm.DOCDATE BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        AND rm.CURTRXAM > 0
    ORDER BY rm.CUSTNMBR, rm.DOCDATE
    """
    return ejecutar_query_alternativa(pais, query)


def listar_stock_almacen_lote_alternativo(pais: str) -> Optional[pd.DataFrame]:
    """Función alternativa para Lista Stock por Almacén y Lote"""
    query = """
    SELECT 
        RTRIM(iv.ITEMNMBR) AS [Código Producto],
        RTRIM(iv.ITEMDESC) AS [Descripción],
        RTRIM(iv00102.LOCNCODE) AS [Almacén],
        RTRIM(iv00102.LOTNUMBR) AS [Lote],
        iv00102.QTYONHND AS [Cantidad],
        iv00102.DATERECD AS [Fecha Recepción],
        iv00102.EXPNDATE AS [Fecha Vencimiento]
    FROM IV00102 iv00102 WITH (NOLOCK)
    INNER JOIN IV00101 iv WITH (NOLOCK) ON iv00102.ITEMNMBR = iv.ITEMNMBR
    WHERE iv00102.QTYONHND > 0
    ORDER BY iv.ITEMDESC, iv00102.LOCNCODE, iv00102.LOTNUMBR
    """
    return ejecutar_query_alternativa(pais, query)


def obtener_precio_lista_alternativo(pais: str) -> Optional[pd.DataFrame]:
    """Función alternativa para Obtener Precio Lista"""
    query = """
    SELECT 
        RTRIM(PRCLEVEL) AS [Nivel Precio],
        RTRIM(ITEMNMBR) AS [Código Producto],
        RTRIM(UOFM) AS [Unidad Medida],
        CURNCYID AS [Moneda],
        LISTPRCE AS [Precio Lista],
        STNDCOST AS [Costo Estándar]
    FROM IV00108 WITH (NOLOCK)
    ORDER BY ITEMNMBR, PRCLEVEL
    """
    return ejecutar_query_alternativa(pais, query)


def listar_documento_vta_detallada_alternativo(pais: str, fecha_inicio: str, fecha_fin: str) -> Optional[pd.DataFrame]:
    """Función alternativa para Lista Documento Vta Detallada"""
    query = f"""
    SELECT 
        RTRIM(h.SOPNUMBE) AS [Número Documento],
        h.SOPTYPE AS [Tipo],
        h.DOCDATE AS [Fecha],
        RTRIM(h.CUSTNMBR) AS [Código Cliente],
        RTRIM(c.CUSTNAME) AS [Nombre Cliente],
        RTRIM(d.ITEMNMBR) AS [Código Producto],
        RTRIM(d.ITEMDESC) AS [Descripción Producto],
        d.QUANTITY AS [Cantidad],
        d.UNITPRCE AS [Precio Unitario],
        d.XTNDPRCE AS [Precio Extendido],
        RTRIM(d.UOFM) AS [Unidad Medida],
        RTRIM(h.BACHNUMB) AS [Número Lote],
        h.SUBTOTAL AS [Subtotal],
        h.TAXAMNT AS [Impuesto],
        h.DOCAMNT AS [Total]
    FROM SOP30200 h WITH (NOLOCK)
    INNER JOIN SOP30300 d WITH (NOLOCK) ON h.SOPNUMBE = d.SOPNUMBE AND h.SOPTYPE = d.SOPTYPE
    LEFT JOIN RM00101 c WITH (NOLOCK) ON h.CUSTNMBR = c.CUSTNMBR
    WHERE h.DOCDATE BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    ORDER BY h.DOCDATE, h.SOPNUMBE, d.LNITMSEQ
    """
    return ejecutar_query_alternativa(pais, query)


def listar_diferencia_precios_alternativo(pais: str, fecha_inicio: str, fecha_fin: str) -> Optional[pd.DataFrame]:
    """Función alternativa para Lista Diferencia Precios"""
    query = f"""
    SELECT 
        RTRIM(d.ITEMNMBR) AS [Código Producto],
        RTRIM(d.ITEMDESC) AS [Descripción],
        RTRIM(h.SOPNUMBE) AS [Número Documento],
        h.DOCDATE AS [Fecha],
        RTRIM(h.CUSTNMBR) AS [Código Cliente],
        d.UNITPRCE AS [Precio Venta],
        i.LISTPRCE AS [Precio Lista],
        (d.UNITPRCE - i.LISTPRCE) AS [Diferencia],
        CASE 
            WHEN i.LISTPRCE > 0 THEN ((d.UNITPRCE - i.LISTPRCE) / i.LISTPRCE) * 100 
            ELSE 0 
        END AS [% Diferencia]
    FROM SOP30200 h WITH (NOLOCK)
    INNER JOIN SOP30300 d WITH (NOLOCK) ON h.SOPNUMBE = d.SOPNUMBE AND h.SOPTYPE = d.SOPTYPE
    LEFT JOIN IV00101 i WITH (NOLOCK) ON d.ITEMNMBR = i.ITEMNMBR
    WHERE h.DOCDATE BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        AND ABS(d.UNITPRCE - ISNULL(i.LISTPRCE, 0)) > 0.01
    ORDER BY h.DOCDATE, d.ITEMNMBR
    """
    return ejecutar_query_alternativa(pais, query)


def listar_fillrate_alternativo(pais: str, fecha_inicio: str, fecha_fin: str) -> Optional[pd.DataFrame]:
    """Función alternativa para Fill Rate por Cliente Producto"""
    query = f"""
    SELECT 
        RTRIM(h.CUSTNMBR) AS [Código Cliente],
        RTRIM(c.CUSTNAME) AS [Nombre Cliente],
        RTRIM(d.ITEMNMBR) AS [Código Producto],
        RTRIM(d.ITEMDESC) AS [Descripción Producto],
        SUM(d.QUANTITY) AS [Cantidad Pedida],
        SUM(d.QTYTOINV) AS [Cantidad Entregada],
        SUM(d.QUANTITY - d.QTYTOINV) AS [Cantidad Pendiente],
        CASE 
            WHEN SUM(d.QUANTITY) > 0 THEN (SUM(d.QTYTOINV) / SUM(d.QUANTITY)) * 100 
            ELSE 0 
        END AS [Fill Rate %]
    FROM SOP30200 h WITH (NOLOCK)
    INNER JOIN SOP30300 d WITH (NOLOCK) ON h.SOPNUMBE = d.SOPNUMBE AND h.SOPTYPE = d.SOPTYPE
    LEFT JOIN RM00101 c WITH (NOLOCK) ON h.CUSTNMBR = c.CUSTNMBR
    WHERE h.DOCDATE BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    GROUP BY h.CUSTNMBR, c.CUSTNAME, d.ITEMNMBR, d.ITEMDESC
    ORDER BY h.CUSTNMBR, d.ITEMNMBR
    """
    return ejecutar_query_alternativa(pais, query)


def reporte_libro_diario_alternativo(pais: str, fecha_inicio: str, fecha_fin: str) -> Optional[pd.DataFrame]:
    """Función alternativa para Reporte Libro Diario"""
    query = f"""
    SELECT 
        RTRIM(JRNENTRY) AS [Número Asiento],
        TRXDATE AS [Fecha Transacción],
        RTRIM(REFRENCE) AS [Referencia],
        RTRIM(ACTNUMST) AS [Cuenta Contable],
        RTRIM(DSCRIPTN) AS [Descripción],
        DEBITAMT AS [Débito],
        CRDTAMNT AS [Crédito],
        RTRIM(ORGNTSRC) AS [Origen],
        RTRIM(ORMSTRID) AS [ID Maestro],
        RTRIM(ORMSTRNM) AS [Nombre Maestro]
    FROM GL20000 WITH (NOLOCK)
    WHERE TRXDATE BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    ORDER BY TRXDATE, JRNENTRY, SEQNUMBR
    """
    return ejecutar_query_alternativa(pais, query)


def reporte_libro_mayor_alternativo(pais: str, fecha_inicio: str, fecha_fin: str) -> Optional[pd.DataFrame]:
    """Función alternativa para Reporte Libro Mayor"""
    query = f"""
    SELECT 
        RTRIM(ACTNUMST) AS [Cuenta Contable],
        RTRIM(ACTDESCR) AS [Descripción Cuenta],
        TRXDATE AS [Fecha],
        RTRIM(JRNENTRY) AS [Asiento],
        RTRIM(REFRENCE) AS [Referencia],
        DEBITAMT AS [Débito],
        CRDTAMNT AS [Crédito],
        (DEBITAMT - CRDTAMNT) AS [Saldo Movimiento]
    FROM GL20000 WITH (NOLOCK)
    WHERE TRXDATE BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    ORDER BY ACTNUMST, TRXDATE, JRNENTRY
    """
    return ejecutar_query_alternativa(pais, query)


def cuenta_contraloria_alternativo(pais: str, fecha_inicio: str, fecha_fin: str) -> Optional[pd.DataFrame]:
    """Función alternativa para Cuenta Contraloría"""
    query = f"""
    SELECT 
        RTRIM(gl.ACTNUMST) AS [Cuenta],
        RTRIM(a.ACTDESCR) AS [Descripción],
        gl.TRXDATE AS [Fecha],
        RTRIM(gl.REFRENCE) AS [Referencia],
        gl.DEBITAMT AS [Débito],
        gl.CRDTAMNT AS [Crédito],
        RTRIM(gl.ORGNTSRC) AS [Origen],
        RTRIM(gl.ORMSTRID) AS [Documento Origen]
    FROM GL20000 gl WITH (NOLOCK)
    LEFT JOIN GL00100 a WITH (NOLOCK) ON gl.ACTINDX = a.ACTINDX
    WHERE gl.TRXDATE BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        AND gl.ACTNUMST LIKE '1%'  -- Cuentas de activo, ajustar según necesidad
    ORDER BY gl.ACTNUMST, gl.TRXDATE
    """
    return ejecutar_query_alternativa(pais, query)


def ejecutar_con_fallback(pais: str, sp_name: str, params: List = None, func_alternativa = None) -> Optional[pd.DataFrame]:
    """
    Intenta ejecutar SP, si no existe usa función alternativa de Python
    
    Args:
        pais: País del servidor
        sp_name: Nombre del stored procedure
        params: Parámetros del SP
        func_alternativa: Función Python alternativa a ejecutar
    """
    # Primero intentar ejecutar el SP
    df = ejecutar_sp(pais, sp_name, params)
    
    # Si falla y hay función alternativa, usarla
    if df is None and func_alternativa is not None:
        st.info(f"🔄 {pais}: Usando función Python alternativa...")
        try:
            if params:
                df = func_alternativa(pais, *params)
            else:
                df = func_alternativa(pais)
        except Exception as e:
            st.error(f"❌ Error en función alternativa: {str(e)}")
            return None
    
    return df


# ============================================================================
# DEFINICIÓN DE STORED PROCEDURES
# ============================================================================

STORED_PROCEDURES = {
    '📊 Reporte Único de Ventas': {
        'sp_name': 'uspGC_RptReporteUnicoDeVentasMACROS',
        'params': ['fecha_inicio', 'fecha_fin'],
        'description': 'Lista documentos de ventas en un período específico.\n\n✅ La columna EAN se agrega automáticamente mediante JOIN a maeGC_ProductoEquiv.',
        'funcion_alternativa': reporte_ventas_alternativo
    },
    '📈 Reporte Único de Ventas Sellin': {
        'sp_name': 'uspGC_RptReporteUnicoDeVentasSellinMACROS',
        'params': ['fecha_inicio', 'fecha_fin'],
        'description': 'Reporte de ventas sell-in con detalle completo.\n\n✅ La columna EAN se agrega automáticamente mediante JOIN a maeGC_ProductoEquiv.',
        'funcion_alternativa': reporte_ventas_alternativo
    },
    '🏪 Reporte Único de Ventas Mercado': {
        'sp_name': 'uspGC_RptReporteUnicoDeVentasMercadoMACROS',
        'params': ['fecha_inicio', 'fecha_fin'],
        'description': 'Reporte de ventas por mercado.\n\n✅ La columna EAN se agrega automáticamente mediante JOIN a maeGC_ProductoEquiv.',
        'funcion_alternativa': reporte_ventas_alternativo
    },
    '👥 Listar Clientes': {
        'sp_name': 'uspGC_ListarClientesMACROS',
        'params': [],
        'description': 'Listado completo de clientes',
        'funcion_alternativa': listar_clientes_alternativo
    },
    '📦 Listar Productos Detallado': {
        'sp_name': 'uspGC_ListarProductoDetalladoMACROS',
        'params': [],
        'description': 'Listado detallado de productos con códigos equivalentes',
        'funcion_alternativa': listar_productos_alternativo
    },
    '📋 Lista Stock por Almacén y Lote': {
        'sp_name': 'uspGC_ListarStockXAlmacenLoteMACROS',
        'params': [],
        'description': 'Inventario por almacén y lote con fechas de vencimiento',
        'funcion_alternativa': listar_stock_almacen_lote_alternativo
    },
    '💰 Obtener Precio Lista': {
        'sp_name': 'uspGC_ObtenerPrecioListaMACROS',
        'params': [],
        'description': 'Lista de precios activa por cliente y producto',
        'funcion_alternativa': obtener_precio_lista_alternativo
    },
    '💵 Reporte Cartera': {
        'sp_name': 'usp_ReporteCarteraMACROS',
        'params': ['fecha_inicio', 'fecha_fin'],
        'description': 'Estado de cartera de clientes',
        'funcion_alternativa': reporte_cartera_alternativo
    },
    '📑 Lista Documento Vta Detallada': {
        'sp_name': 'uspGC_ListarDocumentoVtaDetalladaMACROS',
        'params': ['fecha_inicio', 'fecha_fin'],
        'description': 'Documentos de venta con detalle completo',
        'funcion_alternativa': listar_documento_vta_detallada_alternativo
    },
    '💲 Lista Diferencia Precios': {
        'sp_name': 'uspGC_ListarDiferenciaPreciosMACROS',
        'params': ['fecha_inicio', 'fecha_fin'],
        'description': 'Análisis de diferencias de precios',
        'funcion_alternativa': listar_diferencia_precios_alternativo
    },
    '📊 Fill Rate por Cliente Producto': {
        'sp_name': 'uspGC_ListarFillRateXClienteProductoMACROS',
        'params': ['fecha_inicio', 'fecha_fin'],
        'description': 'Análisis de fill rate por cliente y producto',
        'funcion_alternativa': listar_fillrate_alternativo
    },
    '📖 Reporte Libro Diario': {
        'sp_name': 'usp_ReporteLibroDiariMACROS',
        'params': ['fecha_inicio', 'fecha_fin'],
        'description': 'Libro diario contable',
        'funcion_alternativa': reporte_libro_diario_alternativo
    },
    '📗 Reporte Libro Mayor': {
        'sp_name': 'usp_ReporteLibroMayorMACROS',
        'params': ['fecha_inicio', 'fecha_fin'],
        'description': 'Libro mayor contable',
        'funcion_alternativa': reporte_libro_mayor_alternativo
    },
    '🏦 Cuenta Contraloría': {
        'sp_name': 'uspGC_CuentaContraloriaMACROS',
        'params': ['fecha_inicio', 'fecha_fin'],
        'description': 'Reporte de cuenta contraloría',
        'funcion_alternativa': cuenta_contraloria_alternativo
    }
}


def exportar_a_csv(df: pd.DataFrame, nombre_archivo: str) -> bytes:
    """Exporta DataFrame a CSV en bytes para descarga"""
    return df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')


def guardar_csv_en_carpeta(df: pd.DataFrame, pais: str, nombre_reporte: str, timestamp: str) -> str:
    """Guarda CSV en la carpeta del país y retorna la ruta"""
    pais_dir = os.path.join(BASE_DIR, pais)
    nombre_archivo = f"{nombre_reporte.replace(' ', '_')}_{timestamp}.csv"
    ruta_completa = os.path.join(pais_dir, nombre_archivo)
    
    df.to_csv(ruta_completa, index=False, encoding='utf-8-sig')
    return ruta_completa


def exportar_a_excel(dfs: Dict[str, pd.DataFrame], nombre_archivo: str) -> bytes:
    """Exporta múltiples DataFrames a Excel con hojas separadas"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in dfs.items():
            # Truncar nombres de hoja a 31 caracteres (límite Excel)
            safe_name = sheet_name[:31]
            
            # Limpiar datos para evitar caracteres ilegales en Excel
            df_clean = df.copy()
            for col in df_clean.columns:
                if df_clean[col].dtype == 'object':
                    # Reemplazar caracteres ilegales (control characters 0x00-0x1F excepto tab, newline, carriage return)
                    df_clean[col] = df_clean[col].astype(str).str.replace(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', regex=True)
            
            df_clean.to_excel(writer, sheet_name=safe_name, index=False)
    return output.getvalue()


# ============================================================================
# INTERFAZ DE USUARIO
# ============================================================================

def main():
    # Título principal
    st.title("📊 Sistema de Reportes SQL Server")
    st.markdown("---")
    
    # ========================================
    # SIDEBAR - Configuración
    # ========================================
    with st.sidebar:
        st.header("⚙️ Configuración")
        
        # Selección de países
        st.subheader("🌎 Países")
        paises_seleccionados = st.multiselect(
            "Seleccionar países:",
            options=list(SERVERS_CONFIG.keys()),
            default=['CHILE'],
            help="Selecciona uno o más países para ejecutar los reportes"
        )
        
        if not paises_seleccionados:
            st.warning("⚠️ Debes seleccionar al menos un país")
            return
        
        st.markdown("---")
        
        # Selección de reporte
        st.subheader("📋 Reportes Disponibles")
        reporte_seleccionado = st.selectbox(
            "Seleccionar reporte:",
            options=list(STORED_PROCEDURES.keys()),
            help="Elige el reporte que deseas ejecutar"
        )
        
        st.markdown("---")
        
        # Información del reporte
        st.info(f"**Descripción:**\n\n{STORED_PROCEDURES[reporte_seleccionado]['description']}")
        
        # Mostrar SP name
        with st.expander("🔧 Detalles técnicos"):
            st.code(f"SP: {STORED_PROCEDURES[reporte_seleccionado]['sp_name']}")
            st.write(f"Parámetros: {STORED_PROCEDURES[reporte_seleccionado]['params']}")
        
        st.markdown("---")
        
        # Botón de descarga de tablas
        st.subheader("📥 Descarga de Tablas")
        if st.button("⬇️ Descargar Tablas Base", use_container_width=True, help="Descarga las tablas necesarias para los reportes (últimos 36 meses)"):
            st.session_state['descargar_tablas'] = True
        
        # Mostrar info de metadatos si está disponible
        if METADATA_DISPONIBLE:
            with st.expander("ℹ️ Info de Metadatos"):
                st.caption(f"📅 Análisis: {ESTADISTICAS_ANALISIS['fecha_analisis'][:10]}")
                st.caption(f"📋 Tablas: {ESTADISTICAS_ANALISIS['tablas_analizadas']}")
                
                # Resumen de diferencias por país
                st.markdown("**Diferencias detectadas:**")
                for pais, stats in ESTADISTICAS_ANALISIS['diferencias_totales'].items():
                    col1, col2 = st.columns([1, 2])
                    with col1:
                        st.write(f"🌎 **{pais}**")
                    with col2:
                        st.write(f"{stats['total']} total | "
                               f"❌ {stats.get('COLUMN_MISSING', 0)} faltantes | "
                               f"➕ {stats.get('COLUMN_EXTRA', 0)} extras")
                
                st.caption("💡 Sistema maneja automáticamente estas diferencias")
        
        # Mostrar info de hashing si está disponible
        if HASHING_DISPONIBLE:
            st.markdown("---")
            with st.expander("🔒 Control de Integridad"):
                try:
                    stats_hash = obtener_estadisticas_control()
                    
                    st.metric("📊 Total Controles", stats_hash['total_registros'])
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("📋 Tablas", stats_hash['tablas_unicas'])
                    with col2:
                        st.metric("🌎 Países", stats_hash['paises_unicos'])
                    
                    if stats_hash['modificaciones_detectadas'] > 0:
                        st.warning(f"⚠️ {stats_hash['modificaciones_detectadas']} modificaciones detectadas")
                    else:
                        st.success("✅ Sin modificaciones detectadas")
                    
                    if stats_hash['ultima_actualizacion']:
                        st.caption(f"🕐 Última actualización: {stats_hash['ultima_actualizacion'][:19]}")
                    
                except Exception as e:
                    st.caption(f"⚠️ Error al cargar estadísticas de hash: {str(e)}")
            
            # Botón para ver historial de hashes
            if st.button("📜 Ver Historial de Hashes", use_container_width=True):
                st.session_state['ver_historial_hash'] = True
    
    # ========================================
    # ÁREA PRINCIPAL - Parámetros y Ejecución
    # ========================================
    
    # Mostrar historial de hashes si se solicitó
    if st.session_state.get('ver_historial_hash', False):
        st.header("📜 Historial de Control de Integridad")
        
        try:
            historial = leer_historial_completo()
            
            if historial.empty:
                st.info("ℹ️ No hay historial de hashes aún. Descarga tablas para comenzar el seguimiento.")
            else:
                # Filtros
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    pais_filtro = st.selectbox(
                        "Filtrar por país:",
                        options=["Todos"] + sorted(historial['pais'].unique().tolist()),
                        key="hist_pais"
                    )
                
                with col2:
                    tabla_filtro = st.selectbox(
                        "Filtrar por tabla:",
                        options=["Todas"] + sorted(historial['tabla'].unique().tolist()),
                        key="hist_tabla"
                    )
                
                with col3:
                    estado_filtro = st.selectbox(
                        "Filtrar por estado:",
                        options=["Todos", "inicial", "actualizado_incremental", "modificacion_detectada"],
                        key="hist_estado"
                    )
                
                # Aplicar filtros
                df_filtrado = historial.copy()
                if pais_filtro != "Todos":
                    df_filtrado = df_filtrado[df_filtrado['pais'] == pais_filtro]
                if tabla_filtro != "Todas":
                    df_filtrado = df_filtrado[df_filtrado['tabla'] == tabla_filtro]
                if estado_filtro != "Todos":
                    df_filtrado = df_filtrado[df_filtrado['estado'] == estado_filtro]
                
                # Mostrar tabla
                st.dataframe(
                    df_filtrado[['pais', 'tabla', 'version', 'estado', 'total_registros', 'timestamp', 'hash_sha256']],
                    use_container_width=True,
                    hide_index=True
                )
                
                # Métricas
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Registros", len(df_filtrado))
                with col2:
                    modificados = len(df_filtrado[df_filtrado['estado'] == 'modificacion_detectada'])
                    st.metric("Modificaciones", modificados)
                with col3:
                    versiones = df_filtrado['version'].sum()
                    st.metric("Total Versiones", int(versiones))
                with col4:
                    st.metric("Tablas Únicas", df_filtrado['tabla'].nunique())
        
        except Exception as e:
            st.error(f"❌ Error al cargar historial: {str(e)}")
        
        if st.button("🔙 Volver", use_container_width=True):
            st.session_state['ver_historial_hash'] = False
            st.rerun()
        
        st.stop()  # Detener ejecución aquí sin continuar
    
    # Verificar si se debe ejecutar descarga de tablas
    if st.session_state.get('descargar_tablas', False):
        descargar_todas_las_tablas(paises_seleccionados)
        st.session_state['descargar_tablas'] = False
        st.rerun()  # Recargar página después de descarga
        st.stop()
    
    # Obtener configuración del reporte seleccionado
    config = STORED_PROCEDURES[reporte_seleccionado]
    params_names = config['params']
    
    # Título del reporte
    st.header(reporte_seleccionado)
    st.markdown(f"*{config['description']}*")
    st.markdown("---")
    
    # ========================================
    # Captura de parámetros
    # ========================================
    params_values = []
    
    if params_names:
        st.subheader("📝 Parámetros de Entrada")
        
        col1, col2 = st.columns(2)
        
        if 'fecha_inicio' in params_names:
            with col1:
                fecha_inicio = st.date_input(
                    "📅 Fecha Inicio",
                    value=datetime.now().replace(day=1),
                    help="Fecha de inicio del período"
                )
                params_values.append(fecha_inicio.strftime('%Y-%m-%d'))
        
        if 'fecha_fin' in params_names:
            with col2:
                fecha_fin = st.date_input(
                    "📅 Fecha Fin",
                    value=datetime.now(),
                    help="Fecha de fin del período"
                )
                params_values.append(fecha_fin.strftime('%Y-%m-%d'))
        
        st.markdown("---")
    
    # ========================================
    # Botón de ejecución
    # ========================================
    col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 2])
    
    with col_btn1:
        ejecutar = st.button("▶️ Ejecutar Reporte", type="primary", use_container_width=True)
    
    with col_btn2:
        limpiar = st.button("🗑️ Limpiar Resultados", use_container_width=True)
    
    if limpiar:
        st.rerun()
    
    # ========================================
    # Ejecución y resultados
    # ========================================
    
    if ejecutar:
        st.markdown("---")
        st.subheader("📊 Resultados")
        
        resultados = {}
        archivos_guardados = []
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        total_paises = len(paises_seleccionados)
        
        for idx, pais in enumerate(paises_seleccionados):
            status_text.text(f"Ejecutando en {pais}...")
            
            with st.spinner(f"Procesando {pais}..."):
                # Usar función con fallback si existe función alternativa
                func_alt = config.get('funcion_alternativa')
                df = ejecutar_con_fallback(
                    pais, 
                    config['sp_name'], 
                    params_values if params_values else None,
                    func_alt
                )
                
                if df is not None and not df.empty:
                    resultados[pais] = df
                    
                    # Guardar CSV en carpeta del país
                    try:
                        ruta_guardada = guardar_csv_en_carpeta(
                            df, 
                            pais, 
                            reporte_seleccionado,
                            timestamp
                        )
                        archivos_guardados.append({
                            'pais': pais,
                            'ruta': ruta_guardada,
                            'registros': len(df)
                        })
                    except Exception as e:
                        st.warning(f"⚠️ No se pudo guardar archivo en carpeta {pais}: {e}")
                    
                    # Mostrar resultados por país
                    with st.expander(f"🌎 {pais} - {len(df):,} registros", expanded=True):
                        # Métricas
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("📊 Registros", f"{len(df):,}")
                        with col2:
                            st.metric("📋 Columnas", len(df.columns))
                        with col3:
                            st.metric("💾 Tamaño", f"{df.memory_usage(deep=True).sum() / 1024:.1f} KB")
                        with col4:
                            st.metric("💾 Guardado", "✅" if any(a['pais'] == pais for a in archivos_guardados) else "❌")
                        
                        # Tabla de datos
                        st.dataframe(
                            df,
                            use_container_width=True,
                            height=400
                        )
                        
                        # Mostrar ruta del archivo guardado
                        archivo_info = next((a for a in archivos_guardados if a['pais'] == pais), None)
                        if archivo_info:
                            st.success(f"📁 Guardado en: `{archivo_info['ruta']}`")
                        
                        # Botón de descarga individual
                        csv = exportar_a_csv(df, f"{reporte_seleccionado}_{pais}")
                        st.download_button(
                            label=f"📥 Descargar CSV - {pais}",
                            data=csv,
                            file_name=f"{reporte_seleccionado.replace(' ', '_')}_{pais}_{timestamp}.csv",
                            mime="text/csv",
                            key=f"download_{pais}"
                        )
                elif df is not None:
                    st.warning(f"⚠️ {pais}: No se encontraron datos")
            
            # Actualizar progress bar
            progress_bar.progress((idx + 1) / total_paises)
        
        status_text.text("✅ Ejecución completada")
        
        # ========================================
        # Descarga consolidada
        # ========================================
        
        if resultados:
            st.markdown("---")
            
            # Mostrar resumen de archivos guardados
            if archivos_guardados:
                st.subheader("📁 Archivos Guardados en Carpetas")
                
                df_archivos = pd.DataFrame(archivos_guardados)
                df_archivos.columns = ['País', 'Ruta Completa', 'Registros']
                
                st.dataframe(
                    df_archivos,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Ruta Completa": st.column_config.TextColumn(
                            "Ruta Completa",
                            width="large"
                        ),
                        "Registros": st.column_config.NumberColumn(
                            "Registros",
                            format="%d"
                        )
                    }
                )
                
                st.info(f"💡 Los archivos se han guardado automáticamente en las carpetas de cada país dentro de `{BASE_DIR}`")
            
            st.markdown("---")
            st.subheader("💾 Descarga Consolidada")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Excel con múltiples hojas
                excel_data = exportar_a_excel(resultados, f"{reporte_seleccionado}_consolidado")
                st.download_button(
                    label="📊 Descargar Excel Consolidado (múltiples hojas)",
                    data=excel_data,
                    file_name=f"{reporte_seleccionado.replace(' ', '_')}_consolidado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
            with col2:
                # CSV consolidado con columna de país
                df_consolidado = pd.concat(
                    [df.assign(PAIS=pais) for pais, df in resultados.items()],
                    ignore_index=True
                )
                csv_consolidado = exportar_a_csv(df_consolidado, f"{reporte_seleccionado}_consolidado")
                st.download_button(
                    label="📄 Descargar CSV Consolidado (todos los países)",
                    data=csv_consolidado,
                    file_name=f"{reporte_seleccionado.replace(' ', '_')}_consolidado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            
            # Resumen estadístico
            st.markdown("---")
            st.subheader("📈 Resumen Estadístico")
            
            resumen_data = []
            for pais, df in resultados.items():
                resumen_data.append({
                    'País': pais,
                    'Registros': len(df),
                    'Columnas': len(df.columns),
                    'Tamaño (KB)': df.memory_usage(deep=True).sum() / 1024
                })
            
            df_resumen = pd.DataFrame(resumen_data)
            st.dataframe(df_resumen, use_container_width=True, hide_index=True)
            
            # Totales
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("🌎 Total Países", len(resultados))
            with col2:
                st.metric("📊 Total Registros", f"{df_resumen['Registros'].sum():,}")
            with col3:
                st.metric("💾 Tamaño Total", f"{df_resumen['Tamaño (KB)'].sum():.1f} KB")
        else:
            st.warning("⚠️ No se obtuvieron resultados de ningún país")
    
    # ========================================
    # Footer
    # ========================================
    st.markdown("---")
    st.caption(f"Sistema de Reportes SQL Server | Última actualización: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
