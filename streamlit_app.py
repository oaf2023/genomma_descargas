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
import importlib.util

# Importar funciones de app_reportes_sql.py
spec = importlib.util.spec_from_file_location("app_reportes_sql", Path(__file__).parent / "app_reportes_sql.py")
app_sql = importlib.util.module_from_spec(spec)
spec.loader.exec_module(app_sql)

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
    /* Ocultar TODOS los elementos del header y toolbar */
    #MainMenu {visibility: hidden !important;}
    header {visibility: hidden !important;}
    footer {visibility: hidden !important;}
    
    /* Ocultar toolbar completo con todos los botones */
    [data-testid="stToolbar"] {
        visibility: hidden !important;
        display: none !important;
    }
    
    /* Ocultar decoraciones y badges */
    [data-testid="stDecoration"] {display: none !important;}
    [data-testid="stStatusWidget"] {display: none !important;}
    
    /* Ocultar "Hosted with Streamlit" y botón de GitHub */
    [data-testid="stAppViewBlockContainer"] > div:first-child {
        display: none !important;
    }
    
    /* Ocultar elementos específicos de GitHub */
    a[href*="github.com"] {display: none !important;}
    button[kind="header"] {display: none !important;}
    
    /* Ocultar TODOS los badges y enlaces del footer */
    footer {visibility: hidden !important;}
    footer:after {content: ''; visibility: hidden; display: none;}
    .viewerBadge_container__1QSob {display: none !important;}
    .viewerBadge_link__1S137 {display: none !important;}
    .viewerBadge_text__1JaDK {display: none !important;}
    
    /* Ocultar elementos en esquina inferior */
    [data-testid="stBottom"] {display: none !important;}
    [class*="viewerBadge"] {display: none !important;}
    
    /* Ocultar botones de gestión */
    [data-testid="manage-app-button"] {display: none !important;}
    [data-testid="deploy-button"] {display: none !important;}
    
    /* Ocultar cualquier iframe o elemento externo */
    iframe[title*="GitHub"] {display: none !important;}
    iframe[title*="Streamlit"] {display: none !important;}
    
    /* Ocultar elementos con clase st-emotion */
    [class*="st-emotion"][class*="eqpbllx"] {display: none !important;}
    
    /* Ocultar header actions */
    [data-testid="stHeaderActionElements"] {display: none !important;}
    
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
            ["🏠 Inicio", "📊 Explorar Datos", "💻 Query SQL", "🔧 Pipeline ETL", "📈 Reportes SQL Server", "⚙️ Configuración"],
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

def pagina_reportes_sql():
    """Página para ejecutar reportes de SQL Server"""
    st.markdown('<h1 class="main-header">📈 Reportes SQL Server</h1>', unsafe_allow_html=True)
    
    st.markdown("""
    Esta sección permite ejecutar **stored procedures** y descargar **tablas** desde los servidores SQL Server 
    de los diferentes países (Chile, Colombia, Ecuador, Perú).
    
    **Características:**
    - ✅ Ejecución de 14 stored procedures diferentes
    - 📥 Descarga de tablas completas (últimos 36 meses)
    - 🔒 Control de integridad con hashing
    - 📊 Exportación a CSV y Excel
    - 🌎 Soporte multi-país
    """)
    
    st.markdown("---")
    
    # ========================================
    # SIDEBAR - Configuración
    # ========================================
    with st.sidebar:
        st.markdown("---")
        st.header("⚙️ Configuración SQL Server")
        
        # Selección de países
        st.subheader("🌎 Países")
        paises_seleccionados = st.multiselect(
            "Seleccionar países:",
            options=list(app_sql.SERVERS_CONFIG.keys()),
            default=['CHILE'],
            help="Selecciona uno o más países para ejecutar los reportes",
            key="sql_paises"
        )
        
        if not paises_seleccionados:
            st.warning("⚠️ Debes seleccionar al menos un país")
            return
        
        st.markdown("---")
        
        # Selección de reporte
        st.subheader("📋 Reportes Disponibles")
        reporte_seleccionado = st.selectbox(
            "Seleccionar reporte:",
            options=list(app_sql.STORED_PROCEDURES.keys()),
            help="Elige el reporte que deseas ejecutar",
            key="sql_reporte"
        )
        
        st.markdown("---")
        
        # Información del reporte
        st.info(f"**Descripción:**\n\n{app_sql.STORED_PROCEDURES[reporte_seleccionado]['description']}")
        
        # Mostrar SP name
        with st.expander("🔧 Detalles técnicos"):
            st.code(f"SP: {app_sql.STORED_PROCEDURES[reporte_seleccionado]['sp_name']}")
            st.write(f"Parámetros: {app_sql.STORED_PROCEDURES[reporte_seleccionado]['params']}")
        
        st.markdown("---")
        
        # Botón de descarga de tablas
        st.subheader("📥 Descarga de Tablas")
        if st.button("⬇️ Descargar Tablas Base", use_container_width=True, help="Descarga las tablas necesarias para los reportes (últimos 36 meses)", key="sql_descargar"):
            st.session_state['sql_descargar_tablas'] = True
        
        # Mostrar info de metadatos si está disponible
        if app_sql.METADATA_DISPONIBLE:
            with st.expander("ℹ️ Info de Metadatos"):
                st.caption(f"📅 Análisis: {app_sql.ESTADISTICAS_ANALISIS['fecha_analisis'][:10]}")
                st.caption(f"📋 Tablas: {app_sql.ESTADISTICAS_ANALISIS['tablas_analizadas']}")
        
        # Mostrar info de hashing si está disponible
        if app_sql.HASHING_DISPONIBLE:
            st.markdown("---")
            with st.expander("🔒 Control de Integridad"):
                st.info("💡 Sistema de hashing disponible pero requiere configuración adicional")
            
            # Botón para ver historial de hashes
            if st.button("📜 Ver Historial de Hashes", use_container_width=True, key="sql_ver_historial"):
                st.session_state['sql_ver_historial_hash'] = True
    
    # ========================================
    # ÁREA PRINCIPAL - Parámetros y Ejecución
    # ========================================
    
    # Mostrar historial de hashes si se solicitó
    if st.session_state.get('sql_ver_historial_hash', False):
        st.header("📜 Historial de Control de Integridad")
        st.info("ℹ️ Esta funcionalidad requiere configuración adicional del sistema de hashing")
        
        if st.button("🔙 Volver", use_container_width=True, key="sql_volver_historial"):
            st.session_state['sql_ver_historial_hash'] = False
            st.rerun()
        
        st.stop()  # Detener ejecución aquí sin continuar
    
    # Verificar si se debe ejecutar descarga de tablas
    if st.session_state.get('sql_descargar_tablas', False):
        app_sql.descargar_todas_las_tablas(paises_seleccionados)
        st.session_state['sql_descargar_tablas'] = False
        st.rerun()
        st.stop()
    
    # Obtener configuración del reporte seleccionado
    config = app_sql.STORED_PROCEDURES[reporte_seleccionado]
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
                    help="Fecha de inicio del período",
                    key="sql_fecha_inicio"
                )
                params_values.append(fecha_inicio.strftime('%Y-%m-%d'))
        
        if 'fecha_fin' in params_names:
            with col2:
                fecha_fin = st.date_input(
                    "📅 Fecha Fin",
                    value=datetime.now(),
                    help="Fecha de fin del período",
                    key="sql_fecha_fin"
                )
                params_values.append(fecha_fin.strftime('%Y-%m-%d'))
        
        st.markdown("---")
    
    # ========================================
    # Botón de ejecución
    # ========================================
    col_btn1, col_btn2 = st.columns([1, 1])
    
    with col_btn1:
        ejecutar = st.button("▶️ Ejecutar Reporte", type="primary", use_container_width=True, key="sql_ejecutar")
    
    with col_btn2:
        limpiar = st.button("🗑️ Limpiar Resultados", use_container_width=True, key="sql_limpiar")
    
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
                df = app_sql.ejecutar_con_fallback(
                    pais, 
                    config['sp_name'], 
                    params_values if params_values else None,
                    func_alt
                )
                
                if df is not None and not df.empty:
                    resultados[pais] = df
                    
                    # Guardar CSV en carpeta del país
                    try:
                        ruta_guardada = app_sql.guardar_csv_en_carpeta(
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
                        csv = app_sql.exportar_a_csv(df, f"{reporte_seleccionado}_{pais}")
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
                
                st.info(f"💡 Los archivos se han guardado automáticamente en las carpetas de cada país dentro de `{app_sql.BASE_DIR}`")
            
            st.markdown("---")
            st.subheader("💾 Descarga Consolidada")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Excel con múltiples hojas
                excel_data = app_sql.exportar_a_excel(resultados, f"{reporte_seleccionado}_consolidado")
                st.download_button(
                    label="📊 Descargar Excel Consolidado (múltiples hojas)",
                    data=excel_data,
                    file_name=f"{reporte_seleccionado.replace(' ', '_')}_consolidado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_excel_consolidado"
                )
            
            with col2:
                # CSV consolidado con columna de país
                df_consolidado = pd.concat(
                    [df.assign(PAIS=pais) for pais, df in resultados.items()],
                    ignore_index=True
                )
                csv_consolidado = app_sql.exportar_a_csv(df_consolidado, f"{reporte_seleccionado}_consolidado")
                st.download_button(
                    label="📄 Descargar CSV Consolidado (todos los países)",
                    data=csv_consolidado,
                    file_name=f"{reporte_seleccionado.replace(' ', '_')}_consolidado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    key="download_csv_consolidado"
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
    elif opcion == "📈 Reportes SQL Server":
        pagina_reportes_sql()
    elif opcion == "⚙️ Configuración":
        pagina_configuracion()

if __name__ == "__main__":
    main()

