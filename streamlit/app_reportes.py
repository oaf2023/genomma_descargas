#!/usr/bin/env python3
"""
Genomma Lab - Dashboard Snowflake
Aplicación Streamlit para consultar y analizar datos de Snowflake

Autor: Sistema
Fecha: 2026-01-27
"""

import streamlit as st
import pandas as pd
import snowflake.connector
from datetime import datetime
import os
from pathlib import Path

# ============================================================================
# CONFIGURACIÓN INICIAL
# ============================================================================

# Cargar variables de entorno
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / "etl" / ".env")
except:
    pass

# Configuración de la página
st.set_page_config(
    page_title="Genomma Lab - Dashboard Snowflake",
    page_icon="🌎",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# ESTILOS CSS
# ============================================================================

st.markdown("""
<style>
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1e3c72 0%, #2a5298 100%);
    }
    
    [data-testid="stSidebar"] * {
        color: white !important;
    }
    
    .stButton > button {
        width: 100%;
        border-radius: 8px;
        font-weight: 600;
    }
    
    .main-title {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
        margin-bottom: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# FUNCIONES DE CONEXIÓN
# ============================================================================

@st.cache_resource
def get_connection():
    """Establece conexión con Snowflake"""
    try:
        # Intentar desde st.secrets (Streamlit Cloud)
        if hasattr(st, 'secrets') and "snowflake" in st.secrets:
            config = {
                "account": st.secrets.snowflake.account,
                "user": st.secrets.snowflake.user,
                "password": st.secrets.snowflake.password,
                "warehouse": st.secrets.snowflake.warehouse,
                "database": st.secrets.snowflake.database,
                "schema": st.secrets.snowflake.schema,
            }
            if "role" in st.secrets.snowflake:
                config["role"] = st.secrets.snowflake.role
        else:
            # Usar variables de entorno
            config = {
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "user": os.getenv("SNOWFLAKE_USER"),
                "password": os.getenv("SNOWFLAKE_PASSWORD"),
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
                "database": os.getenv("SNOWFLAKE_DATABASE"),
                "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            }
            role = os.getenv("SNOWFLAKE_ROLE")
            if role:
                config["role"] = role
        
        # Validar configuración
        required = ["account", "user", "password", "warehouse", "database", "schema"]
        missing = [k for k in required if not config.get(k)]
        
        if missing:
            st.error(f"❌ Faltan configuraciones: {', '.join(missing)}")
            return None
        
        # Conectar
        conn = snowflake.connector.connect(**config)
        return conn
    
    except Exception as e:
        st.error(f"❌ Error de conexión: {str(e)}")
        return None


@st.cache_data(ttl=300)
def run_query(query: str):
    """Ejecuta una query y retorna DataFrame"""
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"❌ Error en query: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def get_tables():
    """Obtiene lista de tablas disponibles"""
    query = """
    SELECT TABLE_NAME, TABLE_SCHEMA, ROW_COUNT
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = CURRENT_SCHEMA()
      AND TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME
    """
    return run_query(query)


# ============================================================================
# SIDEBAR
# ============================================================================

with st.sidebar:
    st.title("📋 Menú Principal")
    st.markdown("---")
    
    page = st.radio(
        "Navegación:",
        ["🏠 Inicio", "📊 Explorar Datos", "🔍 Query SQL", "🔧 Pipeline ETL", "⚙️ Configuración"],
        label_visibility="collapsed"
    )
    
    st.markdown("---")
    st.markdown("### 🔌 Conexión")
    
    conn = get_connection()
    if conn:
        st.success("✅ Conectado")
        try:
            test_df = pd.read_sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()", conn)
            st.caption(f"**DB:** {test_df.iloc[0, 0]}")
            st.caption(f"**Schema:** {test_df.iloc[0, 1]}")
        except:
            pass
    else:
        st.error("❌ Sin conexión")
        st.caption("Ve a Configuración")
    
    st.markdown("---")
    st.caption(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# ============================================================================
# PÁGINA: INICIO
# ============================================================================

if page == "🏠 Inicio":
    st.markdown('<div class="main-title">🌎 Genomma Lab - Dashboard Snowflake</div>', unsafe_allow_html=True)
    
    st.markdown("### 👋 Bienvenido")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.info("**📊 Explorar Datos**\n\nVisualiza y analiza tablas de Snowflake")
    
    with col2:
        st.info("**🔍 Query SQL**\n\nEjecuta consultas personalizadas")
    
    with col3:
        st.info("**🔧 Pipeline ETL**\n\nEjecuta proceso de carga de datos")
    
    with col4:
        st.info("**⚙️ Configuración**\n\nGestiona credenciales y conexión")
    
    if conn:
        st.markdown("---")
        st.markdown("### 📋 Tablas Disponibles")
        
        tables_df = get_tables()
        
        if not tables_df.empty:
            st.metric("Total de tablas", len(tables_df))
            
            with st.expander("Ver lista completa"):
                st.dataframe(tables_df, use_container_width=True, hide_index=True)
        else:
            st.warning("No se encontraron tablas")
    else:
        st.warning("⚠️ Configura la conexión para ver las tablas disponibles")

# ============================================================================
# PÁGINA: EXPLORAR DATOS
# ============================================================================

elif page == "📊 Explorar Datos":
    st.markdown('<div class="main-title">📊 Explorar Datos</div>', unsafe_allow_html=True)
    
    if not conn:
        st.error("❌ No hay conexión a Snowflake")
        st.info("👉 Ve a **Configuración** para configurar las credenciales")
        st.stop()
    
    # Obtener tablas
    tables_df = get_tables()
    
    if tables_df.empty:
        st.warning("⚠️ No hay tablas disponibles")
        st.stop()
    
    # Selector de tabla
    col1, col2 = st.columns([2, 1])
    
    with col1:
        selected_table = st.selectbox(
            "Selecciona una tabla:",
            options=tables_df['TABLE_NAME'].tolist()
        )
    
    with col2:
        limit = st.number_input(
            "Límite de filas:",
            min_value=10,
            max_value=10000,
            value=1000,
            step=100
        )
    
    if st.button("📥 Cargar Datos", type="primary"):
        with st.spinner("Cargando datos..."):
            query = f'SELECT * FROM "{selected_table}" LIMIT {limit}'
            df = run_query(query)
        
        if not df.empty:
            st.success(f"✅ {len(df)} filas cargadas")
            
            # Información de la tabla
            col1, col2, col3 = st.columns(3)
            col1.metric("Filas", len(df))
            col2.metric("Columnas", len(df.columns))
            col3.metric("Última carga", datetime.now().strftime("%H:%M:%S"))
            
            # Tabs
            tab1, tab2, tab3 = st.tabs(["📄 Datos", "📊 Estadísticas", "🔍 Filtros"])
            
            with tab1:
                st.dataframe(df, use_container_width=True, height=500)
                
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "📥 Descargar CSV",
                    csv,
                    f"{selected_table}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    "text/csv"
                )
            
            with tab2:
                cols_num = df.select_dtypes(include=['int64', 'float64']).columns
                if len(cols_num) > 0:
                    st.dataframe(df[cols_num].describe(), use_container_width=True)
                else:
                    st.info("No hay columnas numéricas")
            
            with tab3:
                if len(df.columns) > 0:
                    col_filter = st.selectbox("Columna:", df.columns)
                    unique_vals = df[col_filter].unique()[:100]
                    
                    values = st.multiselect("Valores:", unique_vals)
                    if values:
                        filtered = df[df[col_filter].isin(values)]
                        st.dataframe(filtered, use_container_width=True)
        else:
            st.warning("La tabla está vacía")

# ============================================================================
# PÁGINA: QUERY SQL
# ============================================================================

elif page == "🔍 Query SQL":
    st.markdown('<div class="main-title">🔍 Query SQL Personalizada</div>', unsafe_allow_html=True)
    
    if not conn:
        st.error("❌ No hay conexión a Snowflake")
        st.info("👉 Ve a **Configuración** para configurar las credenciales")
        st.stop()
    
    query = st.text_area(
        "Escribe tu consulta SQL:",
        value="SELECT * FROM INFORMATION_SCHEMA.TABLES LIMIT 10",
        height=200
    )
    
    col1, col2 = st.columns([1, 4])
    
    with col1:
        if st.button("▶️ Ejecutar", type="primary"):
            with st.spinner("Ejecutando..."):
                df = run_query(query)
            
            if not df.empty:
                st.success(f"✅ {len(df)} filas × {len(df.columns)} columnas")
                st.dataframe(df, use_container_width=True, height=500)
                
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "📥 Descargar Resultados",
                    csv,
                    f"query_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    "text/csv"
                )
            else:
                st.warning("Sin resultados")

# ============================================================================
# PÁGINA: PIPELINE ETL
# ============================================================================

elif page == "🔧 Pipeline ETL":
    st.markdown('<div class="main-title">🔧 Pipeline ETL - Proceso de Carga</div>', unsafe_allow_html=True)
    
    st.markdown("""
    ### 📋 Pipeline de Extracción, Transformación y Carga
    
    Ejecuta los scripts del proceso ETL en orden:
    1. **Descargar** datos de SQL Server
    2. **Normalizar** headers de archivos CSV
    3. **Renombrar** archivos según estándar
    4. **Cargar** datos a Snowflake
    """)
    
    st.markdown("---")
    
    # Definir scripts
    scripts = [
        {
            "num": 1,
            "name": "Descargar SQL Server",
            "file": "1_descargar_sql_server.py",
            "desc": "Descarga datos desde SQL Server a archivos CSV",
            "icon": "⬇️"
        },
        {
            "num": 2,
            "name": "Normalizar Headers",
            "file": "2_normalizar_headers.py",
            "desc": "Normaliza los nombres de columnas en archivos CSV",
            "icon": "🔤"
        },
        {
            "num": 3,
            "name": "Renombrar Archivos",
            "file": "3_renombrar_archivos.py",
            "desc": "Renombra archivos según convención estándar",
            "icon": "📝"
        },
        {
            "num": 4,
            "name": "Cargar a Snowflake",
            "file": "4_cargar_snowflake.py",
            "desc": "Carga los datos procesados a Snowflake",
            "icon": "⬆️"
        }
    ]
    
    # Tabs para diferentes opciones
    tab1, tab2 = st.tabs(["🚀 Ejecutar Scripts", "📊 Estado"])
    
    with tab1:
        st.markdown("### Selecciona qué ejecutar")
        
        # Opción de ejecutar todo
        if st.button("▶️ Ejecutar Pipeline Completo", type="primary", use_container_width=True):
            progress_bar = st.progress(0)
            status_placeholder = st.empty()
            
            import subprocess
            
            for i, script in enumerate(scripts):
                status_placeholder.info(f"🔄 Ejecutando: {script['icon']} {script['name']}")
                progress_bar.progress((i) / len(scripts))
                
                script_path = Path(__file__).parent.parent / "etl" / script["file"]
                
                try:
                    result = subprocess.run(
                        ["python3", str(script_path)],
                        capture_output=True,
                        text=True,
                        timeout=300
                    )
                    
                    if result.returncode == 0:
                        status_placeholder.success(f"✅ {script['name']} completado")
                        with st.expander(f"Ver salida de {script['name']}"):
                            st.code(result.stdout if result.stdout else "Sin salida")
                    else:
                        status_placeholder.error(f"❌ Error en {script['name']}")
                        st.error(result.stderr if result.stderr else "Error desconocido")
                        break
                        
                except subprocess.TimeoutExpired:
                    status_placeholder.error(f"❌ {script['name']} - Tiempo de ejecución excedido")
                    break
                except Exception as e:
                    status_placeholder.error(f"❌ {script['name']} - Error: {str(e)}")
                    break
                
                progress_bar.progress((i + 1) / len(scripts))
            else:
                status_placeholder.success("🎉 Pipeline completado exitosamente")
                progress_bar.progress(1.0)
        
        st.markdown("---")
        st.markdown("### Ejecutar scripts individualmente")
        
        # Mostrar cada script
        for script in scripts:
            with st.expander(f"{script['icon']} {script['num']}. {script['name']}"):
                st.markdown(f"**Descripción:** {script['desc']}")
                st.markdown(f"**Archivo:** `{script['file']}`")
                
                col1, col2 = st.columns([3, 1])
                
                with col2:
                    if st.button(f"▶️ Ejecutar", key=f"run_{script['num']}"):
                        script_path = Path(__file__).parent.parent / "etl" / script["file"]
                        
                        with st.spinner(f"Ejecutando {script['name']}..."):
                            import subprocess
                            
                            try:
                                result = subprocess.run(
                                    ["python3", str(script_path)],
                                    capture_output=True,
                                    text=True,
                                    timeout=300
                                )
                                
                                if result.returncode == 0:
                                    st.success(f"✅ {script['name']} completado")
                                    if result.stdout:
                                        st.code(result.stdout, language="text")
                                else:
                                    st.error(f"❌ Error en {script['name']}")
                                    if result.stderr:
                                        st.code(result.stderr, language="text")
                            
                            except subprocess.TimeoutExpired:
                                st.error(f"❌ Tiempo de ejecución excedido (5 min)")
                            except Exception as e:
                                st.error(f"❌ Error: {str(e)}")
    
    with tab2:
        st.markdown("### 📊 Estado de Scripts")
        
        # Verificar existencia de scripts
        etl_dir = Path(__file__).parent.parent / "etl"
        
        for script in scripts:
            script_path = etl_dir / script["file"]
            
            col1, col2, col3 = st.columns([3, 1, 1])
            
            with col1:
                st.markdown(f"**{script['icon']} {script['name']}**")
            
            with col2:
                if script_path.exists():
                    st.success("✅ Existe")
                else:
                    st.error("❌ No encontrado")
            
            with col3:
                if script_path.exists():
                    size_kb = script_path.stat().st_size / 1024
                    st.caption(f"{size_kb:.1f} KB")

# ============================================================================
# PÁGINA: CONFIGURACIÓN
# ============================================================================

elif page == "⚙️ Configuración":
    st.markdown('<div class="main-title">⚙️ Configuración</div>', unsafe_allow_html=True)
    
    st.markdown("### 📋 Estado de Conexión")
    
    if conn:
        st.success("✅ Conexión establecida")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Account", os.getenv("SNOWFLAKE_ACCOUNT", "N/A"))
            st.metric("User", os.getenv("SNOWFLAKE_USER", "N/A"))
            st.metric("Warehouse", os.getenv("SNOWFLAKE_WAREHOUSE", "N/A"))
        
        with col2:
            st.metric("Database", os.getenv("SNOWFLAKE_DATABASE", "N/A"))
            st.metric("Schema", os.getenv("SNOWFLAKE_SCHEMA", "N/A"))
            st.metric("Role", os.getenv("SNOWFLAKE_ROLE", "N/A"))
        
        if st.button("🔄 Probar Conexión"):
            try:
                test_df = pd.read_sql(
                    "SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA()",
                    conn
                )
                st.success("✅ Conexión funcionando")
                st.json({
                    "Usuario": test_df.iloc[0, 0],
                    "Database": test_df.iloc[0, 1],
                    "Schema": test_df.iloc[0, 2]
                })
            except Exception as e:
                st.error(f"❌ Error: {e}")
    else:
        st.error("❌ Sin conexión a Snowflake")
        
        st.markdown(f"""
        ### 📝 Configuración Requerida
        
        Edita el archivo: **`etl/.env`**
        
        ```bash
        SNOWFLAKE_ACCOUNT=tu_account
        SNOWFLAKE_USER=tu_usuario
        SNOWFLAKE_PASSWORD=tu_password
        SNOWFLAKE_WAREHOUSE=COMPUTE_WH
        SNOWFLAKE_DATABASE=DEV_LND
        SNOWFLAKE_SCHEMA=_SQL_CHI
        SNOWFLAKE_ROLE=tu_role
        ```
        
        O para Streamlit Cloud, usa **Settings → Secrets**
        """)
        
        with st.expander("🔍 Diagnóstico"):
            config = {
                "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT", "❌ NO CONFIG"),
                "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER", "❌ NO CONFIG"),
                "SNOWFLAKE_PASSWORD": "✅ OK" if os.getenv("SNOWFLAKE_PASSWORD") else "❌ NO CONFIG",
                "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE", "❌ NO CONFIG"),
                "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE", "❌ NO CONFIG"),
                "SNOWFLAKE_SCHEMA": os.getenv("SNOWFLAKE_SCHEMA", "❌ NO CONFIG"),
            }
            
            for key, val in config.items():
                if "NO CONFIG" in val:
                    st.error(f"{key}: {val}")
                else:
                    st.success(f"{key}: {val}")

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: gray; font-size: 12px;'>
    🚀 Genomma Lab Dashboard | Última actualización: 2026-01-27
    </div>
    """,
    unsafe_allow_html=True
)
