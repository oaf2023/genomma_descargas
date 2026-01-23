#!/usr/bin/env python3
"""
Script para normalizar headers de archivos CSV en carpetas de países.
Transforma nombres de columnas según nomenclatura estándar Snowflake.

Procesa archivos desde Google Drive Desktop en: CHILE/, COLOMBIA/, ECUADOR/, PERU/

Autor: Sistema
Fecha: 2026-01-22
"""

import os
import csv
import shutil
from pathlib import Path
from typing import List, Tuple, Dict
import logging
from dotenv import load_dotenv

# Cargar configuración
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURACIÓN
# ============================================================================

# Leer desde .env (cuando instales Drive Desktop)
DRIVE_BASE_DIR = os.getenv("DRIVE_BASE_DIR", r"G:\Mi unidad\ETL_Snowflake")
PAISES_STR = os.getenv("PAISES_FOLDERS", "CHILE,COLOMBIA,ECUADOR,PERU")
PAISES = [p.strip() for p in PAISES_STR.split(",") if p.strip()]

# Fallback a carpeta local para pruebas
if not Path(DRIVE_BASE_DIR).exists():
    logger.warning(f"⚠️  Google Drive no detectado en {DRIVE_BASE_DIR}")
    logger.info("   Usando carpeta local de pruebas...")
    DRIVE_BASE_DIR = r"C:\Ciencia de Datos\otros_datos"


# ============================================================================
# MAPEO DE NORMALIZACIÓN - Snowflake Standard
# ============================================================================

MAPEO_HEADERS = {
    # Campos de facturación
    "Rebate": "REBATE",
    "Rebate Fact.": "REBATE_FACT",
    "Estado Documento": "ESTADO_DOCUMENTO",
    "Estado Anulado": "ESTADO_ANULADO",
    "Tipo": "TIPO",
    "Concepto": "CONCEPTO",
    "Codigo": "CODIGO",
    "Direccion": "DIRECCION",
    "Serie": "SERIE",
    "Correlativo": "CORRELATIVO",
    "Marca": "MARCA",
    "EAN": "EAN",
    "Codigo Producto": "CODIGO_PRODUCTO",
    "Nombre Producto": "NOMBRE_PRODUCTO",
    "Categoria": "CATEGORIA",
    "Numero Cliente": "NUMERO_CLIENTE",
    "Nombre Cliente": "NOMBRE_CLIENTE",
    "Clase Cliente": "CLASE_CLIENTE",
    "Cod. Cliente": "COD_CLIENTE",
    "Nombre": "NOMBRE",
    "Razon": "RAZON_SOCIAL",
    "Razón Social": "RAZON_SOCIAL",
    "RUC": "RUC",
    "RFC": "RFC",
    "NIT": "NIT",
    "DNI": "DNI",
    "Cantidad": "CANTIDAD",
    "Precio": "PRECIO",
    "Subtotal": "SUBTOTAL",
    "Descuento": "DESCUENTO",
    "Total": "TOTAL",
    "Fecha": "FECHA",
    "Fecha Emision": "FECHA_EMISION",
    "Fecha Vencimiento": "FECHA_VENCIMIENTO",
    "Moneda": "MONEDA",
    "TC": "TIPO_CAMBIO",
    "Almacen": "ALMACEN",
    "Vendedor": "VENDEDOR",
    "Zona": "ZONA",
    "Region": "REGION",
    "Pais": "PAIS",
    "Ciudad": "CIUDAD",
    "Departamento": "DEPARTAMENTO",
    "Provincia": "PROVINCIA",
    "Distrito": "DISTRITO",
    "Comuna": "COMUNA",
    "Telefono": "TELEFONO",
    "Email": "EMAIL",
    "Estado": "ESTADO",
    "Observaciones": "OBSERVACIONES",
    "Usuario": "USUARIO",
    "Glosa": "GLOSA"
}


def normalizar_header(header: str) -> str:
    """
    Normaliza un nombre de columna según reglas Snowflake
    
    Args:
        header: Nombre original de columna
    
    Returns:
        Nombre normalizado (uppercase, sin espacios, sin acentos)
    """
    # Buscar en mapeo primero
    if header in MAPEO_HEADERS:
        return MAPEO_HEADERS[header]
    
    # Normalización genérica
    normalizado = header.strip()
    
    # Remover acentos
    import unicodedata
    normalizado = unicodedata.normalize('NFKD', normalizado)
    normalizado = ''.join([c for c in normalizado if not unicodedata.combining(c)])
    
    # Reemplazar espacios y caracteres especiales con _
    normalizado = normalizado.replace(" ", "_")
    normalizado = normalizado.replace(".", "_")
    normalizado = normalizado.replace("-", "_")
    
    # Uppercase
    normalizado = normalizado.upper()
    
    # Eliminar guiones bajos múltiples
    while "__" in normalizado:
        normalizado = normalizado.replace("__", "_")
    
    # Remover guiones bajos al inicio/final
    normalizado = normalizado.strip("_")
    
    return normalizado


def normalizar_csv(archivo_path: str, pais: str, dry_run: bool = False) -> Tuple[bool, str]:
    """
    Normaliza headers de un archivo CSV
    
    Args:
        archivo_path: Ruta completa al archivo CSV
        pais: Nombre del país
        dry_run: Si True, solo muestra cambios sin guardar
    
    Returns:
        (éxito, mensaje)
    """
    try:
        archivo_path = Path(archivo_path)
        
        # Leer CSV con encoding UTF-8 BOM
        with open(archivo_path, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.reader(f)
            headers_originales = next(reader)
            filas = list(reader)
        
        # Normalizar headers
        headers_normalizados = [normalizar_header(h) for h in headers_originales]
        
        # Verificar cambios
        cambios = []
        for orig, norm in zip(headers_originales, headers_normalizados):
            if orig != norm:
                cambios.append(f"  {orig} → {norm}")
        
        if not cambios:
            return True, "Sin cambios necesarios"
        
        logger.info(f"📝 {archivo_path.name}:")
        for cambio in cambios[:5]:  # Mostrar primeros 5
            logger.info(f"   {cambio}")
        if len(cambios) > 5:
            logger.info(f"   ... y {len(cambios) - 5} más")
        
        if dry_run:
            return True, f"{len(cambios)} cambios detectados (dry-run)"
        
        # Guardar archivo normalizado
        output_path = archivo_path.parent / f"{archivo_path.stem}_normalizado.csv"
        
        with open(output_path, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers_normalizados)
            writer.writerows(filas)
        
        logger.info(f"   ✓ Guardado: {output_path.name}\n")
        return True, f"Normalizado → {output_path.name}"
        
    except Exception as e:
        logger.error(f"❌ Error procesando {archivo_path.name}: {e}")
        return False, str(e)


def procesar_pais(pais: str, dry_run: bool = False) -> Dict:
    """
    Procesa todos los CSV de un país
    
    Args:
        pais: Nombre del país
        dry_run: Si True, solo simula
    
    Returns:
        Dict con estadísticas
    """
    pais_dir = Path(DRIVE_BASE_DIR) / pais
    
    if not pais_dir.exists():
        logger.warning(f"⚠️  Carpeta no encontrada: {pais_dir}")
        return {"procesados": 0, "exitosos": 0, "errores": 0}
    
    # Buscar CSVs (excluir ya normalizados)
    archivos_csv = [
        f for f in pais_dir.glob("*.csv")
        if not f.name.endswith("_normalizado.csv")
    ]
    
    if not archivos_csv:
        logger.info(f"ℹ️  {pais}: No hay archivos CSV para procesar")
        return {"procesados": 0, "exitosos": 0, "errores": 0}
    
    logger.info(f"\n{'='*70}")
    logger.info(f"📁 PAÍS: {pais}")
    logger.info(f"   Carpeta: {pais_dir}")
    logger.info(f"   Archivos: {len(archivos_csv)}")
    logger.info(f"{'='*70}\n")
    
    stats = {"procesados": 0, "exitosos": 0, "errores": 0}
    
    for archivo in archivos_csv:
        stats["procesados"] += 1
        exito, mensaje = normalizar_csv(archivo, pais, dry_run)
        if exito:
            stats["exitosos"] += 1
        else:
            stats["errores"] += 1
    
    return stats


def main(dry_run: bool = False):
    """
    Función principal
    
    Args:
        dry_run: Si True, solo muestra cambios sin ejecutar
    """
    logger.info("="*70)
    logger.info("🚀 NORMALIZACIÓN DE HEADERS CSV - Multi-País")
    logger.info("="*70)
    logger.info(f"📂 Base Drive: {DRIVE_BASE_DIR}")
    logger.info(f"🌎 Países: {', '.join(PAISES)}")
    logger.info(f"🔍 Modo: {'DRY-RUN (simulación)' if dry_run else 'PRODUCCIÓN'}")
    logger.info("="*70 + "\n")
    
    stats_total = {"procesados": 0, "exitosos": 0, "errores": 0}
    
    for pais in PAISES:
        stats = procesar_pais(pais, dry_run)
        stats_total["procesados"] += stats["procesados"]
        stats_total["exitosos"] += stats["exitosos"]
        stats_total["errores"] += stats["errores"]
    
    # Resumen final
    logger.info("\n" + "="*70)
    logger.info("📊 RESUMEN FINAL")
    logger.info("="*70)
    logger.info(f"✓ Archivos procesados: {stats_total['procesados']}")
    logger.info(f"✓ Exitosos: {stats_total['exitosos']}")
    logger.info(f"✗ Errores: {stats_total['errores']}")
    logger.info("="*70)


if __name__ == "__main__":
    import sys
    
    # Detectar --dry-run en argumentos
    dry_run = "--dry-run" in sys.argv or "-d" in sys.argv
    
    main(dry_run=dry_run)
