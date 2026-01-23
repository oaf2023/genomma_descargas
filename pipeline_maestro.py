#!/usr/bin/env python3
"""
PIPELINE MAESTRO - Ejecuta todos los pasos del ETL secuencialmente

Pasos:
1. Verificar configuración
2. Normalizar headers CSV
3. Renombrar archivos
4. Cargar a Snowflake

Uso:
    python pipeline_maestro.py              # Ejecutar todo
    python pipeline_maestro.py --dry-run    # Simular sin ejecutar
    python pipeline_maestro.py --step 2     # Ejecutar solo paso 2

Autor: Sistema
Fecha: 2026-01-22
"""

import os
import sys
import logging
import subprocess
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Directorio base del proyecto
BASE_DIR = Path(__file__).parent
ETL_DIR = BASE_DIR / "etl"

# Cargar configuración
load_dotenv(ETL_DIR / ".env")

# Configurar logging
log_file = BASE_DIR / "logs" / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_file.parent.mkdir(exist_ok=True)

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
# FUNCIONES AUXILIARES
# ============================================================================

def verificar_configuracion() -> bool:
    """
    Verifica que todas las configuraciones necesarias estén presentes
    
    Returns:
        True si todo OK, False si falta algo
    """
    logger.info("=" * 80)
    logger.info("🔍 VERIFICANDO CONFIGURACIÓN")
    logger.info("=" * 80)
    
    errores = []
    
    # 1. Verificar .env existe
    env_path = ETL_DIR / ".env"
    if not env_path.exists():
        errores.append(".env no encontrado. Copia .env.template y configura.")
    else:
        logger.info(f"✓ Archivo .env encontrado")
    
    # 2. Verificar variables obligatorias
    vars_requeridas = [
        "DRIVE_BASE_DIR",
        "PAISES_FOLDERS",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA"
    ]
    
    for var in vars_requeridas:
        valor = os.getenv(var)
        if not valor:
            errores.append(f"Variable {var} no configurada en .env")
        else:
            # No mostrar password completo
            if "PASSWORD" in var:
                logger.info(f"✓ {var} = {'*' * len(valor)}")
            else:
                logger.info(f"✓ {var} = {valor}")
    
    # 3. Verificar Google Drive existe
    drive_dir = Path(os.getenv("DRIVE_BASE_DIR", ""))
    if not drive_dir.exists():
        logger.warning(f"⚠️  Google Drive no encontrado en: {drive_dir}")
        logger.warning("   El pipeline usará carpeta local de fallback")
    else:
        logger.info(f"✓ Google Drive detectado: {drive_dir}")
        
        # Verificar carpetas de países
        paises = os.getenv("PAISES_FOLDERS", "").split(",")
        for pais in paises:
            pais = pais.strip()
            pais_dir = drive_dir / pais
            if pais_dir.exists():
                logger.info(f"  ✓ Carpeta {pais}/ encontrada")
            else:
                logger.warning(f"  ⚠️  Carpeta {pais}/ NO encontrada")
    
    # 4. Verificar dependencias Python
    logger.info("\nVerificando dependencias Python...")
    dependencias = ["polars", "pandas", "pyarrow", "snowflake.connector", "dotenv"]
    
    for dep in dependencias:
        try:
            __import__(dep)
            logger.info(f"✓ {dep} instalado")
        except ImportError:
            errores.append(f"Dependencia {dep} no instalada. Ejecuta: pip install {dep}")
    
    # Resultado
    logger.info("\n" + "=" * 80)
    if errores:
        logger.error("❌ ERRORES DE CONFIGURACIÓN:")
        for error in errores:
            logger.error(f"  - {error}")
        logger.info("=" * 80)
        return False
    else:
        logger.info("✅ CONFIGURACIÓN CORRECTA")
        logger.info("=" * 80)
        return True


def ejecutar_script(script_path: Path, args: list = None, descripcion: str = "") -> bool:
    """
    Ejecuta un script Python
    
    Args:
        script_path: Ruta al script
        args: Argumentos adicionales
        descripcion: Descripción del paso
    
    Returns:
        True si éxito, False si error
    """
    logger.info("\n" + "=" * 80)
    logger.info(f"▶️  {descripcion}")
    logger.info("=" * 80)
    logger.info(f"Script: {script_path.name}")
    
    if not script_path.exists():
        logger.error(f"❌ Script no encontrado: {script_path}")
        return False
    
    # Construir comando
    cmd = [sys.executable, str(script_path)]
    if args:
        cmd.extend(args)
    
    logger.info(f"Comando: {' '.join(cmd)}\n")
    
    try:
        # Ejecutar script
        result = subprocess.run(
            cmd,
            cwd=script_path.parent,
            capture_output=False,  # Mostrar output en tiempo real
            text=True
        )
        
        if result.returncode == 0:
            logger.info(f"\n✅ {descripcion} - COMPLETADO")
            return True
        else:
            logger.error(f"\n❌ {descripcion} - ERROR (Exit code: {result.returncode})")
            return False
    
    except Exception as e:
        logger.error(f"\n❌ Error ejecutando {script_path.name}: {e}")
        return False


# ============================================================================
# PIPELINE PRINCIPAL
# ============================================================================

def ejecutar_pipeline(dry_run: bool = False, solo_paso: int = None):
    """
    Ejecuta el pipeline completo
    
    Args:
        dry_run: Si True, solo simula sin ejecutar
        solo_paso: Si se especifica, ejecuta solo ese paso (1-4)
    """
    inicio = datetime.now()
    
    logger.info("=" * 80)
    logger.info("🚀 PIPELINE ETL - GOOGLE DRIVE + SNOWFLAKE")
    logger.info("=" * 80)
    logger.info(f"Inicio: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Modo: {'DRY-RUN (simulación)' if dry_run else 'PRODUCCIÓN'}")
    if solo_paso:
        logger.info(f"Ejecutando solo paso: {solo_paso}")
    logger.info(f"Log guardado en: {log_file}")
    logger.info("=" * 80)
    
    # Paso 0: Verificar configuración
    if not verificar_configuracion():
        logger.error("\n❌ PIPELINE ABORTADO - Corrige los errores de configuración")
        sys.exit(1)
    
    # Definir pasos
    pasos = [
        {
            "num": 1,
            "script": ETL_DIR / "2_normalizar_headers.py",
            "args": ["--dry-run"] if dry_run else [],
            "desc": "PASO 1: Normalizar Headers CSV"
        },
        {
            "num": 2,
            "script": ETL_DIR / "3_renombrar_archivos.py",
            "args": [] if dry_run else ["--apply"],
            "desc": "PASO 2: Renombrar Archivos"
        },
        {
            "num": 3,
            "script": ETL_DIR / "4_cargar_snowflake.py",
            "args": [],
            "desc": "PASO 3: Cargar a Snowflake"
        }
    ]
    
    # Filtrar si solo_paso está definido
    if solo_paso:
        pasos = [p for p in pasos if p["num"] == solo_paso]
        if not pasos:
            logger.error(f"❌ Paso {solo_paso} no válido. Pasos disponibles: 1-3")
            sys.exit(1)
    
    # Ejecutar pasos
    exitosos = 0
    fallidos = 0
    
    for paso in pasos:
        if dry_run and paso["num"] == 3:
            logger.info("\n" + "=" * 80)
            logger.info(f"⏭️  PASO 3 omitido en modo DRY-RUN")
            logger.info("=" * 80)
            continue
        
        exito = ejecutar_script(
            paso["script"],
            paso["args"],
            paso["desc"]
        )
        
        if exito:
            exitosos += 1
        else:
            fallidos += 1
            
            # Preguntar si continuar
            if not dry_run and fallidos < len(pasos):
                respuesta = input("\n¿Continuar con siguiente paso? (s/n): ")
                if respuesta.lower() != 's':
                    logger.warning("Pipeline detenido por usuario")
                    break
    
    # Resumen final
    fin = datetime.now()
    duracion = fin - inicio
    
    logger.info("\n" + "=" * 80)
    logger.info("📊 RESUMEN DEL PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Pasos ejecutados: {exitosos + fallidos}")
    logger.info(f"✅ Exitosos: {exitosos}")
    logger.info(f"❌ Fallidos: {fallidos}")
    logger.info(f"⏱️  Duración: {duracion}")
    logger.info(f"📁 Log completo: {log_file}")
    logger.info("=" * 80)
    
    if fallidos == 0:
        logger.info("✅ PIPELINE COMPLETADO EXITOSAMENTE")
        if dry_run:
            logger.info("\n💡 Para ejecutar en producción:")
            logger.info("   python pipeline_maestro.py")
    else:
        logger.error("❌ PIPELINE COMPLETADO CON ERRORES")
        logger.error("   Revisa el log para más detalles")
    
    logger.info("=" * 80)


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Punto de entrada principal"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Pipeline ETL Maestro - Google Drive + Snowflake",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python pipeline_maestro.py                  # Ejecutar todo
  python pipeline_maestro.py --dry-run        # Simular sin cambios
  python pipeline_maestro.py --step 2         # Solo normalizar headers
  python pipeline_maestro.py --step 3         # Solo cargar a Snowflake
        """
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simular ejecución sin hacer cambios"
    )
    
    parser.add_argument(
        "--step",
        type=int,
        choices=[1, 2, 3],
        help="Ejecutar solo un paso específico (1=normalizar, 2=renombrar, 3=cargar)"
    )
    
    args = parser.parse_args()
    
    ejecutar_pipeline(
        dry_run=args.dry_run,
        solo_paso=args.step
    )


if __name__ == "__main__":
    main()
