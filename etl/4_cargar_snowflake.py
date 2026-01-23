#!/usr/bin/env python3
"""
Script para cargar archivos CSV normalizados a Snowflake
Procesa archivos *_normalizado.csv desde Google Drive Desktop

IMPORTANTE: 
- Crea backups automáticos ({TABLA}_OLD) antes de reemplazar tablas
- Detecta separador CSV automáticamente (';' o ',')
- Usa Pandas+Polars para máxima compatibilidad

Autor: Sistema
Fecha: 2026-01-22
"""

import os
import sys
import logging
import tempfile
import shutil
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from datetime import datetime

from dotenv import load_dotenv
import polars as pl
import pandas as pd
import pyarrow.parquet as pq
import snowflake.connector
from snowflake.connector import ProgrammingError

# Cargar variables de entorno
load_dotenv()

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

# Leer desde .env
DRIVE_BASE_DIR = os.getenv("DRIVE_BASE_DIR", r"G:\Mi unidad\ETL_Snowflake")
PAISES_STR = os.getenv("PAISES_FOLDERS", "CHILE,COLOMBIA,ECUADOR,PERU")
PAISES = [p.strip() for p in PAISES_STR.split(",") if p.strip()]

# Fallback a local
if not Path(DRIVE_BASE_DIR).exists():
    logging.warning(f"⚠️  Google Drive no detectado en {DRIVE_BASE_DIR}")
    logging.info("   Usando carpeta local de pruebas...")
    DRIVE_BASE_DIR = r"C:\Ciencia de Datos\otros_datos"

BASE_DIR = Path(DRIVE_BASE_DIR)

# Configuración Snowflake desde .env
SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "DEV_LND"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "_SQL_CHI"),
    "role": os.getenv("SNOWFLAKE_ROLE")
}

# Eliminar espacios
for key in SNOWFLAKE_CONFIG:
    if SNOWFLAKE_CONFIG[key] and isinstance(SNOWFLAKE_CONFIG[key], str):
        SNOWFLAKE_CONFIG[key] = SNOWFLAKE_CONFIG[key].strip()

# Configuraciones de carga
CSV_BATCH_SIZE = int(os.getenv("CSV_BATCH_SIZE", "100000"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
TRUNCATE_BEFORE_LOAD = os.getenv("TRUNCATE_BEFORE_LOAD", "False").upper() == "TRUE"

# ============================================================================
# LOGGING
# ============================================================================

# Crear directorio de logs
LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

log_file = LOG_DIR / f"carga_snowflake_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# FUNCIONES DE UTILIDAD
# ============================================================================

def quote_ident(identifier: str) -> str:
    """Escapa identificador para Snowflake"""
    return f'"{identifier}"'


def normalizar_nombre_tabla(nombre_archivo: str, pais: str) -> str:
    """
    Extrae nombre de tabla desde nombre de archivo normalizado
    
    Ejemplos:
        MAEGC_PRODUCTO_CHILE_normalizado.csv → MAEGC_PRODUCTO_CHILE
        👥_Listar_Clientes_CHILE_normalizado.csv → LISTAR_CLIENTES_CHILE
        📊_Reporte_Único_de_Ventas_CHILE_normalizado.csv → REPORTE_UNICO_DE_VENTAS_CHILE
    """
    import unicodedata
    
    # Remover extensión y sufijo _normalizado
    nombre = nombre_archivo.replace("_normalizado.csv", "")
    
    # Normalizar acentos: Único → Unico
    nombre = unicodedata.normalize('NFKD', nombre)
    nombre = ''.join([c for c in nombre if not unicodedata.combining(c)])
    
    # Limpiar caracteres especiales (emojis, símbolos)
    nombre_limpio = ''.join(c if c.isalnum() or c == '_' else '_' for c in nombre)
    
    # Eliminar guiones bajos múltiples
    while '__' in nombre_limpio:
        nombre_limpio = nombre_limpio.replace('__', '_')
    
    # Convertir a mayúsculas
    nombre_limpio = nombre_limpio.upper().strip('_')
    
    return nombre_limpio


def mapear_tipo_snowflake(dtype: str) -> str:
    """Mapea tipos de Polars a tipos de Snowflake"""
    dtype_str = str(dtype).lower()
    
    if 'int' in dtype_str or 'uint' in dtype_str:
        return "INTEGER"
    elif 'float' in dtype_str or 'decimal' in dtype_str:
        return "FLOAT"
    elif 'bool' in dtype_str:
        return "BOOLEAN"
    elif 'date' in dtype_str:
        if 'datetime' in dtype_str or 'timestamp' in dtype_str:
            return "TIMESTAMP_NTZ"
        return "DATE"
    elif 'time' in dtype_str:
        return "TIME"
    else:
        return "VARCHAR(16777216)"


# ============================================================================
# CONEXIÓN SNOWFLAKE
# ============================================================================

def get_snowflake_connection():
    """Establece conexión con Snowflake"""
    try:
        missing = [k for k, v in SNOWFLAKE_CONFIG.items() if not v]
        if missing:
            raise ValueError(f"Faltan variables de entorno: {', '.join(missing)}")
        
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        logger.info(f"✓ Conectado a Snowflake: {SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}")
        return conn
    
    except Exception as e:
        logger.error(f"Error conectando a Snowflake: {e}")
        raise


# ============================================================================
# OPERACIONES SNOWFLAKE
# ============================================================================

def crear_tabla_snowflake(conn, schema: str, tabla: str, df: pl.DataFrame):
    """
    Crea tabla en Snowflake basada en el esquema del DataFrame Polars
    ANTES de crear, guarda backup de tabla existente como {TABLA}_OLD
    """
    cursor = conn.cursor()
    
    try:
        full_table = f"{quote_ident(schema)}.{quote_ident(tabla)}"
        tabla_old = f"{tabla}_OLD"
        full_table_old = f"{quote_ident(schema)}.{quote_ident(tabla_old)}"
        
        # 1. Verificar si tabla existe
        check_sql = f"SHOW TABLES LIKE '{tabla}' IN SCHEMA {quote_ident(schema)}"
        cursor.execute(check_sql)
        tabla_existe = len(cursor.fetchall()) > 0
        
        if tabla_existe:
            logger.info(f"⚠️  Tabla {tabla} existe. Creando backup...")
            
            # 2. Borrar _OLD anterior si existe
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {full_table_old}")
                logger.info(f"   ✓ Tabla antigua {tabla_old} eliminada")
            except:
                pass
            
            # 3. Renombrar tabla actual a _OLD
            try:
                cursor.execute(f"ALTER TABLE {full_table} RENAME TO {quote_ident(tabla_old)}")
                logger.info(f"   ✓ Backup creado: {tabla} → {tabla_old}")
            except Exception as e:
                logger.warning(f"   No se pudo renombrar a _OLD: {e}")
        
        # 4. Crear tabla nueva con estructura del CSV
        columnas_ddl = []
        for col_name, dtype in zip(df.columns, df.dtypes):
            tipo_sf = mapear_tipo_snowflake(dtype)
            columnas_ddl.append(f"{quote_ident(col_name)} {tipo_sf}")
        
        ddl = f"CREATE TABLE {full_table} (\n  {',\n  '.join(columnas_ddl)}\n)"
        
        logger.info(f"   Creando tabla nueva: {full_table}")
        cursor.execute(ddl)
        logger.info(f"   ✓ Tabla creada: {tabla} ({len(columnas_ddl)} columnas)")
        
    except Exception as e:
        logger.error(f"❌ Error creando tabla {tabla}: {e}")
        raise
    finally:
        cursor.close()


def copy_parquet_to_snowflake(conn, schema: str, tabla: str, parquet_path: str):
    """Carga archivo Parquet a Snowflake usando PUT + COPY INTO"""
    cursor = conn.cursor()
    
    try:
        full_table = f"{quote_ident(schema)}.{quote_ident(tabla)}"
        
        # PUT archivo a stage interno
        logger.info(f"   Subiendo Parquet a stage Snowflake...")
        put_cmd = f"PUT file://{parquet_path} @~/staged_data AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        cursor.execute(put_cmd)
        
        # COPY INTO desde stage
        parquet_filename = Path(parquet_path).name
        copy_cmd = f"""
        COPY INTO {full_table}
        FROM @~/staged_data/{parquet_filename}
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = ABORT_STATEMENT
        """
        
        logger.info(f"   Ejecutando COPY INTO...")
        cursor.execute(copy_cmd)
        
        # Limpiar stage
        cursor.execute(f"REMOVE @~/staged_data/{parquet_filename}")
        
        logger.info(f"   ✓ Datos cargados exitosamente a {tabla}")
        
    except Exception as e:
        logger.error(f"❌ Error en COPY INTO para {tabla}: {e}")
        raise
    finally:
        cursor.close()


# ============================================================================
# CARGA DE ARCHIVOS
# ============================================================================

def cargar_csv_a_snowflake(conn, csv_path: Path, pais: str, schema: str) -> Dict:
    """Carga un archivo CSV normalizado a Snowflake"""
    logger.info(f"\n{'='*80}")
    logger.info(f"📄 Procesando: {csv_path.name} ({pais})")
    logger.info(f"{'='*80}")
    
    stats = {
        'archivo': csv_path.name,
        'pais': pais,
        'tabla': None,
        'filas_leidas': 0,
        'filas_cargadas': 0,
        'exito': False,
        'error': None
    }
    
    try:
        # Extraer nombre de tabla
        nombre_tabla = normalizar_nombre_tabla(csv_path.name, pais)
        stats['tabla'] = nombre_tabla
        
        logger.info(f"   Nombre tabla destino: {nombre_tabla}")
        
        # Detectar separador automáticamente
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            primera_linea = f.readline()
            separador = ';' if primera_linea.count(';') > primera_linea.count(',') else ','
            logger.info(f"   Separador detectado: '{separador}'")
        
        df = None
        
        try:
            # Pandas (más robusto para CSVs complejos)
            df_pandas = pd.read_csv(
                csv_path,
                sep=separador,
                encoding='utf-8-sig',
                on_bad_lines='skip',
                engine='python',
                quoting=1
            )
            
            df = pl.from_pandas(df_pandas)
            logger.info(f"   ✓ CSV leído con Pandas")
            
        except Exception as e:
            # Fallback a Polars
            logger.warning(f"   Pandas falló, intentando Polars: {str(e)[:80]}")
            df = pl.read_csv(
                csv_path,
                separator=separador,
                ignore_errors=True,
                null_values=["", "NULL", "None", "NaN"],
                infer_schema_length=50000,
                truncate_ragged_lines=True,
                try_parse_dates=False
            )
        
        stats['filas_leidas'] = df.height
        logger.info(f"   ✓ Leídas {df.height:,} filas, {df.width} columnas")
        
        # Verificar columna EAN
        if 'EAN' in df.columns:
            ean_count = df.filter(pl.col('EAN').is_not_null() & (pl.col('EAN') != '')).height
            logger.info(f"   ✓ Columna EAN encontrada ({ean_count:,} valores no vacíos)")
        
        if df.is_empty():
            logger.warning(f"   ⚠️  Archivo vacío, omitiendo carga")
            return stats
        
        # Crear tabla (con backup automático)
        crear_tabla_snowflake(conn, schema, nombre_tabla, df)
        
        # Convertir a Parquet y cargar
        with tempfile.TemporaryDirectory(prefix="snowflake_load_") as tmpdir:
            parquet_path = os.path.join(tmpdir, f"{nombre_tabla}.parquet")
            
            logger.info(f"   Convirtiendo a Parquet...")
            pq.write_table(df.to_arrow(), parquet_path, compression="snappy")
            
            # Cargar a Snowflake
            copy_parquet_to_snowflake(conn, schema, nombre_tabla, parquet_path)
        
        stats['filas_cargadas'] = df.height
        stats['exito'] = True
        
        logger.info(f"✅ ÉXITO: {nombre_tabla} ({df.height:,} filas)\n")
        
    except Exception as e:
        stats['error'] = str(e)
        logger.error(f"❌ ERROR: {csv_path.name}")
        logger.error(f"   {e}\n")
    
    return stats


def procesar_carpeta_pais(conn, pais: str, base_dir: Path, schema: str) -> List[Dict]:
    """Procesa todos los archivos normalizados de un país"""
    pais_dir = base_dir / pais
    
    if not pais_dir.exists():
        logger.warning(f"⚠️  Carpeta no encontrada: {pais_dir}")
        return []
    
    archivos_normalizados = list(pais_dir.glob("*_normalizado.csv"))
    
    if not archivos_normalizados:
        logger.info(f"ℹ️  {pais}: No se encontraron archivos normalizados")
        return []
    
    logger.info(f"\n{'#'*80}")
    logger.info(f"# PAÍS: {pais} - {len(archivos_normalizados)} archivos normalizados")
    logger.info(f"{'#'*80}")
    
    estadisticas = []
    
    for csv_path in sorted(archivos_normalizados):
        stats = cargar_csv_a_snowflake(conn, csv_path, pais, schema)
        estadisticas.append(stats)
    
    return estadisticas


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Función principal"""
    print("=" * 80)
    print("CARGA DE ARCHIVOS NORMALIZADOS A SNOWFLAKE")
    print("Desde Google Drive Desktop")
    print("=" * 80)
    
    logger.info(f"\nDirectorio base: {BASE_DIR}")
    logger.info(f"Países a procesar: {', '.join(PAISES)}")
    logger.info(f"Destino Snowflake: {SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}")
    logger.info(f"Log guardado en: {log_file}")
    
    # Conectar a Snowflake
    try:
        conn = get_snowflake_connection()
    except Exception as e:
        logger.error(f"No se pudo conectar a Snowflake. Abortando.")
        sys.exit(1)
    
    try:
        # Procesar cada país
        todas_estadisticas = []
        
        for pais in PAISES:
            stats_pais = procesar_carpeta_pais(
                conn, 
                pais, 
                BASE_DIR, 
                SNOWFLAKE_CONFIG['schema']
            )
            todas_estadisticas.extend(stats_pais)
        
        # Resumen final
        print("\n" + "=" * 80)
        print("📊 RESUMEN GENERAL DE CARGA")
        print("=" * 80)
        
        exitosos = [s for s in todas_estadisticas if s['exito']]
        con_error = [s for s in todas_estadisticas if s.get('error')]
        
        print(f"\n✅ Archivos cargados exitosamente: {len(exitosos)}")
        print(f"❌ Archivos con error: {len(con_error)}")
        
        if exitosos:
            print("\n📋 Detalle por país:")
            for pais in PAISES:
                stats_pais = [s for s in exitosos if s.get('pais') == pais]
                if stats_pais:
                    total_filas = sum(s.get('filas_cargadas', 0) for s in stats_pais)
                    print(f"  🌎 {pais:10s}: {len(stats_pais):2d} tablas | "
                          f"{total_filas:,} filas cargadas")
            
            total_general = sum(s.get('filas_cargadas', 0) for s in exitosos)
            print(f"\n  📊 TOTAL GENERAL: {total_general:,} filas cargadas")
        
        if con_error:
            print("\n⚠️  Archivos con errores:")
            for s in con_error:
                print(f"  - {s['pais']}/{s['archivo']}")
                print(f"    Error: {s.get('error', 'Desconocido')}")
        
        print("\n" + "=" * 80)
        print("✅ PROCESO FINALIZADO")
        print("=" * 80)
        
    finally:
        conn.close()
        logger.info("\n✓ Conexión a Snowflake cerrada")


if __name__ == "__main__":
    main()
