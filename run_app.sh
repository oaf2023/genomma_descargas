#!/bin/bash
# Script para iniciar Genomma Lab Dashboard
# Uso: ./run_app.sh

echo "🚀 Genomma Lab - Dashboard Snowflake"
echo "===================================="
echo ""

cd "$(dirname "$0")"

# Detener procesos anteriores
echo "🔄 Limpiando procesos anteriores..."
pkill -9 -f "streamlit.*app_reportes" 2>/dev/null
sleep 1

# Verificar archivo
if [ ! -f "streamlit/app_reportes.py" ]; then
    echo "❌ Error: No se encuentra streamlit/app_reportes.py"
    exit 1
fi

# Verificar configuración
if [ ! -f "etl/.env" ] && [ ! -f ".streamlit/secrets.toml" ]; then
    echo "⚠️  Advertencia: No se encontró configuración de Snowflake"
    echo "   Crea etl/.env o .streamlit/secrets.toml con las credenciales"
    echo ""
fi

echo "▶️  Iniciando aplicación..."
echo ""

# Iniciar
exec streamlit run streamlit/app_reportes.py \
    --server.port 8501 \
    --server.address 0.0.0.0 \
    --server.enableCORS false \
    --server.enableXsrfProtection false
