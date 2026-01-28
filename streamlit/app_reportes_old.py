#!/usr/bin/env python3
"""
App Streamlit para consultar datos de Snowflake
Dashboard de reportes multi-país

IMPORTANTE: Esta app está diseñada para:
1. Ejecutarse en Streamlit in Snowflake (recomendado)
2. Ejecutarse en share.streamlit.io (con .env en Secrets)
3. Ejecutarse localmente (con .env)

Autor: Sistema
Fecha: 2026-01-26
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

st.set_page_config(
    page_title="Genomma Lab - Dashboard Snowflake",
    page_icon="🌎",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS Personalizado
st.markdown("""
<style>
    /* Sidebar mejorado */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1e3c72 0%, #2a5298 100%);
    }
    
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] {
        color: white;
    }
    
    /* Radio buttons en sidebar */
    [data-testid="stSidebar"] .stRadio > label {
        color: white !important;
        font-weight: 600;
        font-size: 1rem;
    }
    
    [data-testid="stSidebar"] .stRadio > div {
        background-color: rgba(255, 255, 255, 0.1);
        padding: 0.5rem;
        border-radius: 8px;
    }
    
    [data-testid="stSidebar"] .stRadio label[data-baseweb="radio"] {
        background-color: rgba(255, 255, 255, 0.15);
        padding: 0.8rem 1rem;
        border-radius: 8px;
        margin-bottom: 0.5rem;
        transition: all 0.3s ease;
    }
    
    [data-testid="stSidebar"] .stRadio label[data-baseweb="radio"]:hover {
        background-color: rgba(255, 255, 255, 0.25);
        transform: translateX(5px);
    }
    
    /* Header principal */
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
        border-bottom: 3px solid #1f77b4;
        margin-bottom: 2rem;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding-left: 20px;
        padding-right: 20px;
        background-color: #f0f2f6;
        border-radius: 5px 5px 0 0;
        font-weight: 500;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: #1f77b4;
        color: white;
    }
    
    /* Botones mejorados */
    .stButton > button {
        width: 100%;
        border-radius: 8px;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    }
</style>
""", unsafe_allow_html=True)

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
            return None  # Retornar None en lugar de detener
        
        conn = snowflake.connector.connect(**{k: v for k, v in config.items() if v})
        
        return conn
    
    except Exception as e:
        return None  # Retornar None en caso de error


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
    
    if conn is None:
        return pd.DataFrame()  # Retornar DataFrame vacío si no hay conexión
    
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
# UI - HEADER Y NAVEGACIÓN
# ============================================================================

# Inicializar session state
if 'page' not in st.session_state:
    st.session_state.page = 'home'

# Header principal
st.markdown('<div class="main-header">🌎 Genomma Lab - Dashboard de Datos Snowflake</div>', unsafe_allow_html=True)

# ============================================================================
# SIDEBAR - MENÚ DE NAVEGACIÓN
# ============================================================================

# ============================================================================
# SIDEBAR - MENÚ DE NAVEGACIÓN
# ============================================================================

with st.sidebar:
    st.title("📋 Menú Principal")
    st.markdown("---")
    
    menu_option = st.radio(
        "Navegación:",
        ["🏠 Inicio", "📊 Consultar Datos", "🔍 Query SQL", "⚙️ Configuración"],
        key="menu_principal"
    )
    
    st.markdown("---")
    
    # Estado de conexión
    st.markdown("### 🔌 Estado de Conexión")
    
    conn = get_snowflake_connection()
    
    if conn is not None:
        st.success("✅ **Conectado**")
        st.markdown(f"**🗄️ DB:** `{os.getenv('SNOWFLAKE_DATABASE', 'N/A')}`")
        st.markdown(f"**📂 Schema:** `{os.getenv('SNOWFLAKE_SCHEMA', 'N/A')}`")
        st.markdown(f"**👤 User:** `{os.getenv('SNOWFLAKE_USER', 'N/A')}`")
    else:
        st.error("❌ **Sin conexión**")
        st.markdown("👉 Ve a **⚙️ Configuración**")
    
    st.markdown("---")
    
    # Info de la app
    st.markdown("### ℹ️ Información")
    st.caption(f"📅 {datetime.now().strftime('%Y-%m-%d')}")
    st.caption(f"🕐 {datetime.now().strftime('%H:%M:%S')}")
    
    st.markdown("---")
    
    # Links útiles
    with st.expander("🔗 Links Útiles"):
        st.markdown("""
        - [📖 Documentación Snowflake](https://docs.snowflake.com)
        - [🎨 Streamlit Docs](https://docs.streamlit.io)
        - [💬 Soporte](mailto:support@genommalab.com)
        """)

# ============================================================================
# PÁGINA: INICIO
# ============================================================================

if menu_option == "🏠 Inicio":
    st.markdown("## 👋 Bienvenido al Dashboard de Datos")
    
    st.markdown("""
    ### 🎯 ¿Qué puedes hacer aquí?
    
    Esta aplicación te permite consultar y analizar datos de **Snowflake** de forma interactiva:
    """)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div style='background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 2rem; border-radius: 10px; color: white; text-align: center; min-height: 200px;'>
            <h2>📊</h2>
            <h3>Consultar Datos</h3>
            <p>Explora tablas por país, visualiza datos y descarga reportes en CSV</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style='background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); padding: 2rem; border-radius: 10px; color: white; text-align: center; min-height: 200px;'>
            <h2>🔍</h2>
            <h3>Query SQL</h3>
            <p>Ejecuta consultas SQL personalizadas y analiza resultados</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div style='background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); padding: 2rem; border-radius: 10px; color: white; text-align: center; min-height: 200px;'>
            <h2>📈</h2>
            <h3>Análisis</h3>
            <p>Visualiza estadísticas y gráficos de frecuencia de datos</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    if conn is not None:
        st.markdown("### 📋 Resumen de Tablas Disponibles")
        
        tablas_por_pais = listar_tablas_por_pais()
        
        if tablas_por_pais:
            cols = st.columns(len(tablas_por_pais))
            
            for idx, (pais, tablas) in enumerate(tablas_por_pais.items()):
                with cols[idx]:
                    st.metric(
                        label=f"🌎 {pais}",
                        value=f"{len(tablas)} tablas"
                    )
            
            st.markdown("---")
            
            with st.expander("📋 Ver detalle de todas las tablas"):
                for pais, tablas in tablas_por_pais.items():
                    st.markdown(f"**{pais}:**")
                    for tabla in tablas:
                        st.markdown(f"  - `{tabla}`")
        else:
            st.warning("⚠️ No se encontraron tablas en el schema actual")
    else:
        st.warning("⚠️ Configura la conexión en **⚙️ Configuración** para comenzar")
    
    st.markdown("---")
    st.markdown("""
    ### 🚀 Inicio Rápido
    
    1. **Verifica la conexión** en el menú lateral
    2. **Selecciona "Consultar Datos"** para explorar tablas
    3. **Filtra y descarga** los datos que necesites
    """)

# ============================================================================
# PÁGINA: CONSULTAR DATOS
# ============================================================================

elif menu_option == "📊 Consultar Datos":
    
    if conn is None:
        st.error("❌ No hay conexión a Snowflake")
        st.info("👉 Ve a **⚙️ Configuración** para configurar las credenciales")
        st.stop()
    
    st.markdown("## 📊 Consultar Datos por País y Tabla")
    
    # Selector de país y tabla
    tablas_por_pais = listar_tablas_por_pais()
    
    if not tablas_por_pais:
        st.warning("⚠️ No hay tablas disponibles")
        st.stop()
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        pais_seleccionado = st.selectbox(
            "🌎 Selecciona País:",
            options=list(tablas_por_pais.keys()),
            index=0
        )
    
    tablas_disponibles = tablas_por_pais.get(pais_seleccionado, [])
    
    if not tablas_disponibles:
        st.warning(f"⚠️ No hay tablas disponibles para {pais_seleccionado}")
        st.stop()
    
    with col2:
        tabla_seleccionada = st.selectbox(
            "📋 Selecciona Tabla:",
            options=tablas_disponibles,
            index=0
        )
    
    st.markdown("---")
    
    # Configuración de consulta
    col1, col2, col3 = st.columns(3)
    
    with col1:
        limite_filas = st.number_input(
            "📊 Límite de filas:",
            min_value=10,
            max_value=100000,
            value=1000,
            step=100
        )
    
    with col2:
        mostrar_info = st.checkbox("📈 Mostrar estadísticas", value=True)
    
    with col3:
        auto_cargar = st.checkbox("🔄 Cargar automáticamente", value=True)
    
    cargar_datos = auto_cargar or st.button("▶️ Cargar Datos", type="primary")
    
    if cargar_datos:
        # Estadísticas de tabla
        if mostrar_info:
            st.markdown("### 📈 Información de la Tabla")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                query_count = f'SELECT COUNT(*) AS TOTAL FROM "{tabla_seleccionada}"'
                df_count = ejecutar_query(query_count)
                total_filas = df_count["TOTAL"].iloc[0] if not df_count.empty else 0
                st.metric("📊 Total Filas", f"{total_filas:,}")
            
            with col2:
                query_cols = f"SELECT COUNT(*) AS TOTAL FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tabla_seleccionada}'"
                df_cols = ejecutar_query(query_cols)
                total_cols = df_cols["TOTAL"].iloc[0] if not df_cols.empty else 0
                st.metric("📋 Total Columnas", f"{total_cols:,}")
            
            with col3:
                st.metric("🕐 Última Actualización", datetime.now().strftime("%Y-%m-%d"))
            
            st.markdown("---")
        
        # Cargar datos
        st.markdown(f"### 📄 Datos de `{tabla_seleccionada}`")
        
        query_data = f'SELECT * FROM "{tabla_seleccionada}" LIMIT {limite_filas}'
        
        with st.spinner(f"⏳ Cargando {limite_filas:,} filas..."):
            df = ejecutar_query(query_data)
        
        if df.empty:
            st.warning("⚠️ La tabla está vacía")
        else:
            st.success(f"✅ {len(df):,} filas cargadas correctamente")
            
            # Filtros
            with st.expander("🔍 Aplicar Filtros"):
                columnas_texto = df.select_dtypes(include=['object']).columns.tolist()
                
                if columnas_texto:
                    col_filtro = st.selectbox("Columna:", ["Ninguna"] + columnas_texto)
                    
                    if col_filtro != "Ninguna":
                        valores_unicos = df[col_filtro].unique().tolist()
                        valor_filtro = st.multiselect(
                            f"Valores de {col_filtro}:",
                            options=sorted(valores_unicos[:100])
                        )
                        
                        if valor_filtro:
                            df = df[df[col_filtro].isin(valor_filtro)]
                            st.info(f"✅ Filtrado: {len(df):,} filas")
                else:
                    st.info("No hay columnas de texto para filtrar")
            
            # Mostrar datos
            st.dataframe(df, use_container_width=True, height=500)
            
            # Descargar
            col1, col2 = st.columns([3, 1])
            with col2:
                csv = df.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    label="📥 Descargar CSV",
                    data=csv,
                    file_name=f"{tabla_seleccionada}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
            # Análisis rápido
            st.markdown("---")
            st.markdown("### 📊 Análisis Rápido")
            
            tab1, tab2 = st.tabs(["📈 Estadísticas", "📊 Frecuencias"])
            
            with tab1:
                cols_numericas = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
                
                if cols_numericas:
                    st.dataframe(df[cols_numericas].describe(), use_container_width=True)
                else:
                    st.info("No hay columnas numéricas para analizar")
            
            with tab2:
                if len(df.columns) > 0:
                    col_analisis = st.selectbox(
                        "Selecciona columna:",
                        options=df.columns.tolist()
                    )
                    
                    if col_analisis:
                        top_valores = df[col_analisis].value_counts().head(10)
                        
                        col1, col2 = st.columns([2, 1])
                        
                        with col1:
                            st.bar_chart(top_valores)
                        
                        with col2:
                            st.dataframe(
                                top_valores.reset_index().rename(
                                    columns={'index': col_analisis, col_analisis: 'Frecuencia'}
                                ),
                                use_container_width=True
                            )

# ============================================================================
# PÁGINA: QUERY PERSONALIZADA
# ============================================================================

elif menu_option == "🔍 Query SQL":
    
    if conn is None:
        st.error("❌ No hay conexión a Snowflake")
        st.info("👉 Ve a **⚙️ Configuración** para configurar las credenciales")
        st.stop()
    
    st.markdown("## 🔍 Ejecutar Query SQL Personalizada")
    
    st.info("💡 **Tip:** Escribe tu consulta SQL y presiona el botón para ejecutarla")
    
    # Editor SQL
    query_custom = st.text_area(
        "📝 Escribe tu query SQL:",
        value="SELECT * FROM INFORMATION_SCHEMA.TABLES LIMIT 10",
        height=200,
        help="Escribe cualquier consulta SQL válida para Snowflake"
    )
    
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        ejecutar = st.button("▶️ Ejecutar Query", type="primary", use_container_width=True)
    
    with col2:
        limpiar = st.button("🗑️ Limpiar", use_container_width=True)
        if limpiar:
            st.rerun()
    
    if ejecutar:
        try:
            with st.spinner("⏳ Ejecutando query..."):
                df_custom = ejecutar_query(query_custom)
            
            if df_custom.empty:
                st.warning("⚠️ La consulta no retornó resultados")
            else:
                st.success(f"✅ Query ejecutada correctamente: {len(df_custom):,} filas x {len(df_custom.columns)} columnas")
                
                st.markdown("---")
                st.markdown("### 📊 Resultados")
                
                st.dataframe(df_custom, use_container_width=True, height=500)
                
                # Descarga
                csv_custom = df_custom.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    label="📥 Descargar Resultados CSV",
                    data=csv_custom,
                    file_name=f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
        
        except Exception as e:
            st.error(f"❌ Error ejecutando query: {e}")
            st.code(str(e))

# ============================================================================
# PÁGINA: CONFIGURACIÓN
# ============================================================================

elif menu_option == "⚙️ Configuración":
    
    st.markdown("## ⚙️ Configuración de Conexión a Snowflake")
    
    st.markdown("""
    ### 📋 Estado de la Conexión
    """)
    
    if conn is not None:
        st.success("✅ **Conexión establecida correctamente**")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("🗄️ Database", os.getenv('SNOWFLAKE_DATABASE', 'N/A'))
            st.metric("👤 Usuario", os.getenv('SNOWFLAKE_USER', 'N/A'))
            st.metric("🏢 Warehouse", os.getenv('SNOWFLAKE_WAREHOUSE', 'N/A'))
        
        with col2:
            st.metric("📂 Schema", os.getenv('SNOWFLAKE_SCHEMA', 'N/A'))
            st.metric("🌐 Account", os.getenv('SNOWFLAKE_ACCOUNT', 'N/A'))
            st.metric("👔 Role", os.getenv('SNOWFLAKE_ROLE', 'N/A'))
        
        st.markdown("---")
        
        if st.button("🔄 Probar Conexión"):
            with st.spinner("Probando conexión..."):
                try:
                    test_df = ejecutar_query("SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
                    st.success("✅ Conexión funcionando correctamente")
                    st.json({
                        "Usuario": test_df.iloc[0, 0],
                        "Database": test_df.iloc[0, 1],
                        "Schema": test_df.iloc[0, 2]
                    })
                except Exception as e:
                    st.error(f"❌ Error: {e}")
    
    else:
        st.error("❌ **No hay conexión a Snowflake**")
        
        st.markdown(f"""
        ### 📝 Instrucciones de Configuración
        
        Para conectarte a Snowflake, edita el archivo:
        
        📄 **`etl/.env`**
        
        Ruta completa: `{Path(__file__).parent.parent / "etl" / ".env"}`
        
        #### Parámetros requeridos:
        
        ```bash
        SNOWFLAKE_ACCOUNT=tu_account.region
        SNOWFLAKE_USER=tu_usuario
        SNOWFLAKE_PASSWORD=tu_password
        SNOWFLAKE_WAREHOUSE=COMPUTE_WH
        SNOWFLAKE_DATABASE=DEV_LND
        SNOWFLAKE_SCHEMA=_SQL_CHI
        SNOWFLAKE_ROLE=tu_role
        ```
        """)
        
        st.markdown("---")
        
        st.markdown("### 🔍 Configuración Actual")
        
        config_data = {
            "SNOWFLAKE_ACCOUNT": os.getenv('SNOWFLAKE_ACCOUNT', '❌ NO CONFIGURADO'),
            "SNOWFLAKE_USER": os.getenv('SNOWFLAKE_USER', '❌ NO CONFIGURADO'),
            "SNOWFLAKE_PASSWORD": '✅ Configurada' if os.getenv('SNOWFLAKE_PASSWORD') else '❌ NO CONFIGURADO',
            "SNOWFLAKE_WAREHOUSE": os.getenv('SNOWFLAKE_WAREHOUSE', '❌ NO CONFIGURADO'),
            "SNOWFLAKE_DATABASE": os.getenv('SNOWFLAKE_DATABASE', '❌ NO CONFIGURADO'),
            "SNOWFLAKE_SCHEMA": os.getenv('SNOWFLAKE_SCHEMA', '❌ NO CONFIGURADO'),
            "SNOWFLAKE_ROLE": os.getenv('SNOWFLAKE_ROLE', '❌ NO CONFIGURADO'),
        }
        
        df_config = pd.DataFrame(list(config_data.items()), columns=['Parámetro', 'Valor'])
        st.dataframe(df_config, use_container_width=True, hide_index=True)
        
        st.markdown("---")
        
        st.info("""
        💡 **Tip:** Después de editar el archivo `.env`, reinicia la aplicación para que los cambios surtan efecto.
        """)


# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: gray; font-size: 12px;'>
    🚀 Genomma Lab - Dashboard Snowflake | Actualizado: 2026-01-26
    </div>
    """,
    unsafe_allow_html=True
)
