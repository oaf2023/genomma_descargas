#!/usr/bin/env python3
"""
Genomma Lab - Dashboard Snowflake
Aplicación Streamlit para consultar y analizar datos de Snowflake

Autor: oaf

Fecha: 2026-01-27
"""

import streamlit as st
import pandas as pd
import snowflake.connector
from datetime import datetime
import os
from pathlib import Path
import subprocess
import sys

# ============================================================================
# CONFIGURACIÓN INICIAL
# ============================================================================

# Cargar variables de entorno
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / "etl" / ".env")
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
    /* Ocultar menú hamburguesa y elementos superiores */
    #MainMenu {visibility: hidden;}
    header {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Ocultar botones de GitHub, Deploy, etc. */
    [data-testid="stToolbar"] {display: none;}
    [data-testid="stDecoration"] {display: none;}
    [data-testid="stStatusWidget"] {display: none;}
    
    /* Ocultar footer inferior con logos de Streamlit y GitHub */
    footer {visibility: hidden !important;}
    footer:after {
        content: ''; 
        visibility: hidden;
        display: none;
    }
    
    /* Ocultar "Made with Streamlit" y otros enlaces del footer */
    .viewerBadge_container__1QSob {display: none !important;}
    .viewerBadge_link__1S137 {display: none !important;}
    .viewerBadge_text__1JaDK {display: none !important;}
    
    /* Ocultar elementos en la esquina inferior derecha */
    [data-testid="stBottom"] {display: none;}
    [class*="viewerBadge"] {display: none !important;}
    
    /* Ocultar enlace "Deploy" y "Manage app" */
    [data-testid="manage-app-button"] {display: none;}
    
    /* Sidebar con diseño mejorado */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1e3c72 0%, #2a5298 100%);
    }
    
    [data-testid="stSidebar"] * {
        color: white !important;
    }
    
    /* Botones con gradiente */
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        padding: 0.5rem 1rem;
        border-radius: 0.5rem;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
    }
    
    /* Header principal */
    .main-header {
        font-size: 2.5rem;
        font-weight: 800;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 2rem;
    }
    
    /* Cards de métricas */
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# CONEXIÓN A SNOWFLAKE
# ============================================================================

@st.cache_resource
def get_connection():
    """Establece y retorna una conexión a Snowflake"""
    try:
        # Intentar primero con st.secrets (Streamlit Cloud)
        if hasattr(st, 'secrets') and 'snowflake' in st.secrets:
            conn = snowflake.connector.connect(
                user=st.secrets.snowflake.user,
                password=st.secrets.snowflake.password,
                account=st.secrets.snowflake.account,
                warehouse=st.secrets.snowflake.warehouse,
                database=st.secrets.snowflake.database,
                schema=st.secrets.snowflake.schema,
                role=st.secrets.snowflake.role
            )
        else:
            # Usar variables de entorno
            conn = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
                role=os.getenv("SNOWFLAKE_ROLE")
            )
        return conn
    except Exception as e:
        st.error(f"❌ Error al conectar con Snowflake: {str(e)}")
        return None

@st.cache_data(ttl=300)
def run_query(query: str) -> pd.DataFrame:
    """Ejecuta una query en Snowflake y retorna un DataFrame"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"❌ Error al ejecutar query: {str(e)}")
        return pd.DataFrame()

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def get_tables_list():
    """Obtiene lista de tablas disponibles"""
    query = """
    SELECT TABLE_SCHEMA, TABLE_NAME, ROW_COUNT, BYTES 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA'
    ORDER BY TABLE_SCHEMA, TABLE_NAME
    """
    return run_query(query)

def get_table_preview(schema: str, table: str, limit: int = 100):
    """Obtiene preview de una tabla"""
    query = f'SELECT * FROM "{schema}"."{table}" LIMIT {limit}'
    return run_query(query)

def ejecutar_script_etl(script_name: str, script_path: Path):
    """Ejecuta un script ETL y muestra el output"""
    try:
        with st.spinner(f"Ejecutando {script_name}..."):
            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode == 0:
                st.success(f"✅ {script_name} completado exitosamente")
                if result.stdout:
                    with st.expander("Ver salida"):
                        st.code(result.stdout, language="text")
            else:
                st.error(f"❌ Error en {script_name}")
                if result.stderr:
                    st.code(result.stderr, language="text")
                    
    except subprocess.TimeoutExpired:
        st.error(f"❌ {script_name} excedió el tiempo límite de 5 minutos")
    except Exception as e:
        st.error(f"❌ Error al ejecutar {script_name}: {str(e)}")

# ============================================================================
# MENÚ LATERAL
# ============================================================================

def menu_lateral():
    """Renderiza el menú lateral de navegación"""
    with st.sidebar:
        st.markdown("# 🌎 Genomma Lab")
        st.markdown("### Dashboard Snowflake")
        st.markdown("---")
        
        # Opciones de menú
        opcion = st.radio(
            "Navegación",
            ["🏠 Inicio", "📊 Explorar Datos", "💻 Query SQL", "🔧 Pipeline ETL", "⚙️ Configuración"],
            label_visibility="collapsed"
        )
        
        st.markdown("---")
        st.markdown(f"**Última actualización:**")
        st.markdown(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    return opcion

# ============================================================================
# PÁGINAS
# ============================================================================

def pagina_inicio():
    """Página de inicio con información general"""
    st.markdown('<h1 class="main-header">🌎 Genomma Lab - Dashboard Snowflake</h1>', unsafe_allow_html=True)
    
    # Verificar conexión
    conn = get_connection()
    
    if conn:
        st.success("✅ Conexión exitosa con Snowflake")
        
        # Información de la conexión
        try:
            test_df = pd.read_sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()", conn)
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("📁 Base de Datos", test_df.iloc[0, 0])
            with col2:
                st.metric("📂 Schema", test_df.iloc[0, 1])
                
        except Exception as e:
            st.warning(f"⚠️ No se pudo obtener información de la conexión: {str(e)}")
        
        # Estadísticas rápidas
        st.markdown("### 📈 Estadísticas")
        
        try:
            tables_df = get_tables_list()
            if not tables_df.empty:
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("📊 Total Tablas", len(tables_df))
                
                with col2:
                    total_rows = tables_df['ROW_COUNT'].sum()
                    st.metric("📝 Total Registros", f"{total_rows:,}")
                
                with col3:
                    total_bytes = tables_df['BYTES'].sum()
                    total_mb = total_bytes / (1024 * 1024)
                    st.metric("💾 Tamaño Total", f"{total_mb:.2f} MB")
        except:
            pass
            
    else:
        st.error("❌ No se pudo conectar con Snowflake")
        st.info("💡 Verifica tus credenciales en la sección de Configuración")

def pagina_explorar():
    """Página para explorar tablas"""
    st.markdown("## 📊 Explorar Datos")
    
    tables_df = get_tables_list()
    
    if tables_df.empty:
        st.warning("⚠️ No se encontraron tablas")
        return
    
    # Mostrar lista de tablas
    st.markdown("### 📋 Tablas Disponibles")
    st.dataframe(tables_df, use_container_width=True)
    
    # Selector de tabla
    st.markdown("### 🔍 Previsualizar Tabla")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        tabla_seleccionada = st.selectbox(
            "Selecciona una tabla",
            options=[(row['TABLE_SCHEMA'], row['TABLE_NAME']) for _, row in tables_df.iterrows()],
            format_func=lambda x: f"{x[0]}.{x[1]}"
        )
    
    with col2:
        limite = st.number_input("Límite de filas", min_value=10, max_value=1000, value=100)
    
    if st.button("📥 Cargar Preview", type="primary"):
        if tabla_seleccionada:
            schema, tabla = tabla_seleccionada
            preview_df = get_table_preview(schema, tabla, limite)
            
            if not preview_df.empty:
                st.success(f"✅ Mostrando {len(preview_df)} filas de {schema}.{tabla}")
                st.dataframe(preview_df, use_container_width=True)
                
                # Opción de descarga
                csv = preview_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="💾 Descargar CSV",
                    data=csv,
                    file_name=f"{schema}_{tabla}_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )

def pagina_query():
    """Página para ejecutar queries SQL personalizadas"""
    st.markdown("## 💻 Query SQL Personalizada")
    
    # Editor de query
    query = st.text_area(
        "Escribe tu query SQL:",
        height=200,
        placeholder="SELECT * FROM TABLA LIMIT 100;"
    )
    
    col1, col2 = st.columns([1, 4])
    
    with col1:
        ejecutar = st.button("▶️ Ejecutar", type="primary")
    
    if ejecutar and query.strip():
        try:
            df = run_query(query)
            
            if not df.empty:
                st.success(f"✅ Query ejecutada. {len(df)} filas retornadas")
                st.dataframe(df, use_container_width=True)
                
                # Descarga
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="💾 Descargar CSV",
                    data=csv,
                    file_name=f"query_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            else:
                st.info("ℹ️ La query no retornó resultados")
                
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")

def pagina_pipeline():
    """Página para ejecutar el pipeline ETL"""
    st.markdown("## 🔧 Pipeline ETL")
    
    etl_dir = Path(__file__).parent / "etl"
    
    scripts = [
        ("1️⃣ Descargar SQL Server", "1_descargar_sql_server.py"),
        ("2️⃣ Normalizar Headers", "2_normalizar_headers.py"),
        ("3️⃣ Renombrar Archivos", "3_renombrar_archivos.py"),
        ("4️⃣ Cargar a Snowflake", "4_cargar_snowflake.py")
    ]
    
    st.markdown("### 📋 Scripts Disponibles")
    
    # Ejecutar todos
    if st.button("▶️ Ejecutar Pipeline Completo", type="primary"):
        for nombre, archivo in scripts:
            script_path = etl_dir / archivo
            if script_path.exists():
                ejecutar_script_etl(nombre, script_path)
            else:
                st.error(f"❌ No se encontró: {archivo}")
        st.success("✅ Pipeline completo finalizado")
    
    st.markdown("---")
    st.markdown("### 🎯 Ejecutar Scripts Individuales")
    
    # Ejecutar individual
    for nombre, archivo in scripts:
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.markdown(f"**{nombre}**")
        
        with col2:
            if st.button("▶️", key=archivo):
                script_path = etl_dir / archivo
                if script_path.exists():
                    ejecutar_script_etl(nombre, script_path)
                else:
                    st.error(f"❌ No se encontró: {archivo}")

def pagina_configuracion():
    """Página de configuración"""
    st.markdown("## ⚙️ Configuración")
    
    st.markdown("### 🔐 Credenciales Snowflake")
    
    conn = get_connection()
    
    if conn:
        st.success("✅ Conexión configurada correctamente")
        
        # Mostrar info (sin mostrar password)
        if hasattr(st, 'secrets') and 'snowflake' in st.secrets:
            st.info("📝 Usando credenciales de Streamlit Secrets")
        else:
            st.info("📝 Usando variables de entorno (.env)")
            
    else:
        st.error("❌ Credenciales no configuradas o incorrectas")
        
        st.markdown("""
        **Para configurar las credenciales:**
        
        1. **Local:** Crea un archivo `.streamlit/secrets.toml` con:
        ```toml
        [snowflake]
        user = "tu_usuario"
        password = "tu_password"
        account = "tu_cuenta"
        warehouse = "tu_warehouse"
        database = "tu_database"
        schema = "tu_schema"
        role = "tu_role"
        ```
        
        2. **Streamlit Cloud:** Configura los secrets en la configuración del app
        """)

# ============================================================================
# APLICACIÓN PRINCIPAL
# ============================================================================

def main():
    """Función principal de la aplicación"""
    
    # Menú lateral
    opcion = menu_lateral()
    
    # Renderizar página según selección
    if opcion == "🏠 Inicio":
        pagina_inicio()
    elif opcion == "📊 Explorar Datos":
        pagina_explorar()
    elif opcion == "💻 Query SQL":
        pagina_query()
    elif opcion == "🔧 Pipeline ETL":
        pagina_pipeline()
    elif opcion == "⚙️ Configuración":
        pagina_configuracion()

if __name__ == "__main__":
    main()

