#!/usr/bin/env python3
"""
Script para renombrar archivos CSV normalizados
Remueve timestamps y garantiza nomenclatura consistente con país

Procesa archivos desde Google Drive Desktop

Autor: Sistema
Fecha: 2026-01-22
"""

import os
import re
from pathlib import Path
from typing import List, Tuple
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

DRIVE_BASE_DIR = os.getenv("DRIVE_BASE_DIR", r"G:\Mi unidad\ETL_Snowflake")
PAISES_STR = os.getenv("PAISES_FOLDERS", "CHILE,COLOMBIA,ECUADOR,PERU")
PAISES = [p.strip() for p in PAISES_STR.split(",") if p.strip()]

# Fallback a local
if not Path(DRIVE_BASE_DIR).exists():
    logger.warning(f"⚠️  Google Drive no detectado en {DRIVE_BASE_DIR}")
    logger.info("   Usando carpeta local de pruebas...")
    DRIVE_BASE_DIR = r"C:\Ciencia de Datos\otros_datos"

# Patrón para detectar timestamp: _YYYYMMDD_HHMMSS
TIMESTAMP_PATTERN = r"_\d{8}_\d{6}"


def renombrar_archivos_pais(pais: str, dry_run: bool = True) -> List[Tuple[str, str]]:
    """
    Renombra archivos CSV en la carpeta del país
    
    Args:
        pais: Nombre del país (CHILE, COLOMBIA, etc)
        dry_run: Si True, solo muestra los cambios sin ejecutar
    
    Returns:
        Lista de tuplas (nombre_original, nombre_nuevo)
    """
    pais_dir = Path(DRIVE_BASE_DIR) / pais
    
    if not pais_dir.exists():
        logger.warning(f"⚠️  Carpeta no encontrada: {pais_dir}")
        return []
    
    cambios = []
    archivos_csv = [f for f in pais_dir.glob("*.csv") if f.is_file()]
    
    if not archivos_csv:
        logger.info(f"ℹ️  {pais}: No se encontraron archivos CSV")
        return []
    
    logger.info(f"\n📁 Procesando carpeta: {pais}")
    logger.info(f"   Archivos encontrados: {len(archivos_csv)}")
    
    for archivo in archivos_csv:
        nombre_original = archivo.name
        
        # Saltar si no es archivo normalizado
        if "_normalizado.csv" not in nombre_original:
            continue
        
        # Construir nuevo nombre
        # 1. Remover timestamp si existe
        nombre_nuevo = re.sub(TIMESTAMP_PATTERN, "", nombre_original)
        
        # 2. Garantizar que termine con _PAIS_normalizado.csv
        base_name = nombre_nuevo.replace("_normalizado.csv", "")
        
        # Remover país del nombre si ya existe (evitar duplicados)
        base_name = base_name.replace(f"_{pais}", "")
        
        # Reconstruir con país al final
        nombre_nuevo = f"{base_name}_{pais}_normalizado.csv"
        
        # 3. Limpiar guiones bajos múltiples
        while "__" in nombre_nuevo:
            nombre_nuevo = nombre_nuevo.replace("__", "_")
        
        if nombre_original != nombre_nuevo:
            cambios.append((nombre_original, nombre_nuevo))
            logger.info(f"   📝 {nombre_original}")
            logger.info(f"      → {nombre_nuevo}")
    
    if not cambios:
        logger.info(f"   ✓ No se requieren cambios")
        return []
    
    # Ejecutar renombrado si no es dry-run
    if not dry_run:
        for original, nuevo in cambios:
            ruta_original = pais_dir / original
            ruta_nueva = pais_dir / nuevo
            
            try:
                ruta_original.rename(ruta_nueva)
                logger.info(f"   ✓ Renombrado: {original} → {nuevo}")
            except Exception as e:
                logger.error(f"   ❌ Error renombrando {original}: {e}")
    
    return cambios


def main(dry_run: bool = True):
    """
    Función principal
    
    Args:
        dry_run: Si True, solo muestra cambios sin ejecutar
    """
    logger.info("="*70)
    logger.info("🔧 RENOMBRADO DE ARCHIVOS CSV - Multi-País")
    logger.info("="*70)
    logger.info(f"📂 Base Drive: {DRIVE_BASE_DIR}")
    logger.info(f"🌎 Países: {', '.join(PAISES)}")
    logger.info(f"🔍 Modo: {'DRY-RUN (simulación)' if dry_run else 'PRODUCCIÓN'}")
    logger.info("="*70)
    
    total_cambios = 0
    
    for pais in PAISES:
        cambios = renombrar_archivos_pais(pais, dry_run)
        total_cambios += len(cambios)
    
    logger.info("\n" + "="*70)
    logger.info("📊 RESUMEN")
    logger.info("="*70)
    logger.info(f"Total archivos a renombrar: {total_cambios}")
    
    if dry_run and total_cambios > 0:
        logger.info("\n⚠️  Modo DRY-RUN activado. Para aplicar cambios ejecuta:")
        logger.info("   python 3_renombrar_archivos.py --apply")
    elif not dry_run:
        logger.info(f"✓ Archivos renombrados: {total_cambios}")
    
    logger.info("="*70)


if __name__ == "__main__":
    import sys
    
    # Por defecto dry-run, usar --apply para ejecutar
    dry_run = "--apply" not in sys.argv
    
    main(dry_run=dry_run)
