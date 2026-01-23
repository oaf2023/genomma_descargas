#!/usr/bin/env python3
"""
App Streamlit para consultar datos de Snowflake
Dashboard de reportes multi-país

IMPORTANTE: Esta app está diseñada para:
1. Ejecutarse en Streamlit in Snowflake (recomendado)
2. Ejecutarse en share.streamlit.io (con .env en Secrets)
3. Ejecutarse localmente (con .env)

Autor: Sistema
Fecha: 2026-01-22
"""

import streamlit as st
import pandas as pd
import snowflake.connector
from datetime import datetime, timedelta
import os
from pathlib import Path

# Intentar cargar .env si existe (local)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / "etl" / ".env")
except:
    pass


# ============================================================================
# CONFIGURACIÓN
# ============================================================================

# Título de la app
st.set_page_config(
    page_title="Reportes Snowflake Multi-País",
    page_icon="🌎",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# CONEXIÓN SNOWFLAKE
# ============================================================================

@st.cache_resource
def get_snowflake_connection():
    """
    Establece conexión con Snowflake
    
    Prioridad de configuración:
    1. st.secrets (Streamlit Cloud)
    2. Variables de entorno (.env local)
    """
    try:
        # Intentar desde st.secrets primero (Streamlit Cloud)
        if "snowflake" in st.secrets:
            config = {
                "account": st.secrets.snowflake.account,
                "user": st.secrets.snowflake.user,
                "password": st.secrets.snowflake.password,
                "warehouse": st.secrets.snowflake.warehouse,
                "database": st.secrets.snowflake.database,
                "schema": st.secrets.snowflake.schema,
                "role": st.secrets.snowflake.get("role", None)
            }
        else:
            # Fallback a variables de entorno (local)
            config = {
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "user": os.getenv("SNOWFLAKE_USER"),
                "password": os.getenv("SNOWFLAKE_PASSWORD"),
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
                "database": os.getenv("SNOWFLAKE_DATABASE", "DEV_LND"),
                "schema": os.getenv("SNOWFLAKE_SCHEMA", "_SQL_CHI"),
                "role": os.getenv("SNOWFLAKE_ROLE")
            }
        
        # Validar configuración
        missing = [k for k, v in config.items() if k != "role" and not v]
        if missing:
            st.error(f"❌ Faltan configuraciones: {', '.join(missing)}")
            st.info("💡 Configura las credenciales en .streamlit/secrets.toml o variables de entorno")
            st.stop()
        
        conn = snowflake.connector.connect(**{k: v for k, v in config.items() if v})
        
        return conn
    
    except Exception as e:
        st.error(f"❌ Error conectando a Snowflake: {e}")
        st.stop()


def ejecutar_query(query: str, params: dict = None) -> pd.DataFrame:
    """
    Ejecuta query en Snowflake y retorna DataFrame de Pandas
    
    Args:
        query: SQL query
        params: Parámetros para query parametrizada
    
    Returns:
        DataFrame con resultados
    """
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        # Fetch resultados
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        df = pd.DataFrame(rows, columns=columns)
        return df
    
    finally:
        cursor.close()


def listar_tablas_por_pais() -> dict:
    """
    Lista todas las tablas agrupadas por país
    
    Returns:
        Dict: {pais: [lista_de_tablas]}
    """
    query = """
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = CURRENT_SCHEMA()
      AND TABLE_TYPE = 'BASE TABLE'
      AND TABLE_NAME NOT LIKE '%_OLD'
    ORDER BY TABLE_NAME
    """
    
    df = ejecutar_query(query)
    
    # Agrupar por país (asumiendo que las tablas terminan en _PAIS)
    tablas_por_pais = {
        "CHILE": [],
        "COLOMBIA": [],
        "ECUADOR": [],
        "PERU": [],
        "OTROS": []
    }
    
    for tabla in df["TABLE_NAME"].tolist():
        asignado = False
        for pais in ["CHILE", "COLOMBIA", "ECUADOR", "PERU"]:
            if tabla.endswith(f"_{pais}"):
                tablas_por_pais[pais].append(tabla)
                asignado = True
                break
        
        if not asignado:
            tablas_por_pais["OTROS"].append(tabla)
    
    # Remover países sin tablas
    return {k: v for k, v in tablas_por_pais.items() if v}


# ============================================================================
# UI - SIDEBAR
# ============================================================================

st.sidebar.title("🌎 Reportes Multi-País")
st.sidebar.markdown("---")

# Selector de país
tablas_por_pais = listar_tablas_por_pais()
pais_seleccionado = st.sidebar.selectbox(
    "Selecciona País",
    options=list(tablas_por_pais.keys()),
    index=0
)

# Selector de tabla
tablas_disponibles = tablas_por_pais.get(pais_seleccionado, [])

if not tablas_disponibles:
    st.warning(f"⚠️ No hay tablas disponibles para {pais_seleccionado}")
    st.stop()

tabla_seleccionada = st.sidebar.selectbox(
    "Selecciona Tabla",
    options=tablas_disponibles,
    index=0
)

st.sidebar.markdown("---")

# Opciones de visualización
mostrar_info_tabla = st.sidebar.checkbox("📊 Mostrar información de tabla", value=True)
limite_filas = st.sidebar.number_input(
    "Límite de filas",
    min_value=10,
    max_value=100000,
    value=1000,
    step=100
)

st.sidebar.markdown("---")
st.sidebar.markdown(f"**Conexión:** {os.getenv('SNOWFLAKE_DATABASE', 'N/A')}")
st.sidebar.markdown(f"**Schema:** {os.getenv('SNOWFLAKE_SCHEMA', 'N/A')}")


# ============================================================================
# UI - MAIN CONTENT
# ============================================================================

st.title(f"🌎 {pais_seleccionado} - {tabla_seleccionada}")
st.markdown("---")

# Información de tabla
if mostrar_info_tabla:
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Contar filas
        query_count = f'SELECT COUNT(*) AS TOTAL FROM "{tabla_seleccionada}"'
        df_count = ejecutar_query(query_count)
        total_filas = df_count["TOTAL"].iloc[0] if not df_count.empty else 0
        
        st.metric("📊 Total Filas", f"{total_filas:,}")
    
    with col2:
        # Contar columnas
        query_cols = f'SELECT COUNT(*) AS TOTAL FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = \'{tabla_seleccionada}\''
        df_cols = ejecutar_query(query_cols)
        total_cols = df_cols["TOTAL"].iloc[0] if not df_cols.empty else 0
        
        st.metric("📋 Total Columnas", f"{total_cols:,}")
    
    with col3:
        # Última modificación (aproximada)
        st.metric("🕐 Última Actualización", datetime.now().strftime("%Y-%m-%d"))
    
    st.markdown("---")

# Tabs para diferentes vistas
tab1, tab2, tab3 = st.tabs(["📄 Vista de Datos", "📊 Análisis", "🔍 Query SQL"])

with tab1:
    st.subheader("Vista de Datos")
    
    # Query principal
    query_data = f'SELECT * FROM "{tabla_seleccionada}" LIMIT {limite_filas}'
    
    with st.spinner(f"Cargando {limite_filas:,} filas..."):
        df = ejecutar_query(query_data)
    
    if df.empty:
        st.warning("⚠️ La tabla está vacía")
    else:
        st.success(f"✅ {len(df):,} filas cargadas")
        
        # Filtros básicos (opcional)
        with st.expander("🔍 Filtros"):
            columnas_texto = df.select_dtypes(include=['object']).columns.tolist()
            
            if columnas_texto:
                col_filtro = st.selectbox("Columna a filtrar", ["Ninguna"] + columnas_texto)
                
                if col_filtro != "Ninguna":
                    valores_unicos = df[col_filtro].unique().tolist()
                    valor_filtro = st.multiselect(
                        f"Valores de {col_filtro}",
                        options=valores_unicos[:100]  # Limitar a 100 opciones
                    )
                    
                    if valor_filtro:
                        df = df[df[col_filtro].isin(valor_filtro)]
                        st.info(f"Filtrado: {len(df):,} filas")
        
        # Mostrar datos
        st.dataframe(
            df,
            use_container_width=True,
            height=600
        )
        
        # Descarga
        csv = df.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            label="📥 Descargar CSV",
            data=csv,
            file_name=f"{tabla_seleccionada}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )

with tab2:
    st.subheader("Análisis Exploratorio")
    
    if df.empty:
        st.warning("⚠️ No hay datos para analizar")
    else:
        # Resumen estadístico
        st.markdown("### 📊 Resumen Estadístico")
        
        # Columnas numéricas
        cols_numericas = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
        
        if cols_numericas:
            st.dataframe(df[cols_numericas].describe(), use_container_width=True)
        else:
            st.info("No hay columnas numéricas para analizar")
        
        st.markdown("---")
        
        # Top valores por columna
        st.markdown("### 🔝 Top 10 Valores Frecuentes")
        
        col_analisis = st.selectbox(
            "Selecciona columna para análisis de frecuencia",
            options=df.columns.tolist()
        )
        
        if col_analisis:
            top_valores = df[col_analisis].value_counts().head(10)
            
            col_chart, col_table = st.columns([2, 1])
            
            with col_chart:
                st.bar_chart(top_valores)
            
            with col_table:
                st.dataframe(
                    top_valores.reset_index().rename(columns={'index': col_analisis, col_analisis: 'Frecuencia'}),
                    use_container_width=True
                )

with tab3:
    st.subheader("🔍 Ejecutar Query SQL Personalizada")
    
    # Editor SQL
    query_custom = st.text_area(
        "Escribe tu query SQL",
        value=f'SELECT * FROM "{tabla_seleccionada}" LIMIT 100',
        height=150
    )
    
    ejecutar = st.button("▶️ Ejecutar Query", type="primary")
    
    if ejecutar:
        try:
            with st.spinner("Ejecutando query..."):
                df_custom = ejecutar_query(query_custom)
            
            st.success(f"✅ Query ejecutada: {len(df_custom):,} filas")
            
            st.dataframe(df_custom, use_container_width=True)
            
            # Descarga
            csv_custom = df_custom.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="📥 Descargar Resultados",
                data=csv_custom,
                file_name=f"query_custom_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        
        except Exception as e:
            st.error(f"❌ Error ejecutando query: {e}")


# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: gray; font-size: 12px;'>
    🚀 App Streamlit - Reportes Snowflake Multi-País | Actualizado: 2026-01-22
    </div>
    """,
    unsafe_allow_html=True
)
