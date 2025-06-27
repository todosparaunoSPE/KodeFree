import streamlit as st
import pandas as pd
from databricks import sql
import plotly.express as px
import os

# --- Imagen en sidebar ---
with st.sidebar:
    st.image("logo.png", width=150)

# --- Configuración de conexión a Databricks (usando variables de entorno o secrets) ---
DATABRICKS_SERVER_HOSTNAME = "dbc-8d323cd0-08cd.cloud.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/72b1345bdedbf507"
DATABRICKS_ACCESS_TOKEN = st.secrets["DATABRICKS_ACCESS_TOKEN"]

def query_databricks(query: str) -> pd.DataFrame:
    with sql.connect(
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_ACCESS_TOKEN
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
    return pd.DataFrame(data, columns=columns)

# --- Título y descripción ---
st.title("📊 Dashboard Producción Petrolera - KodeFree Data Engineer Jr")
st.markdown("""
Bienvenido al dashboard interactivo de producción petrolera.  
Aquí puedes explorar datos reales consultados desde **Databricks** y visualizar gráficos dinámicos.
""")

# --- Barra lateral: filtros ---
st.sidebar.header("Filtros")
limite = st.sidebar.slider("Número de registros a mostrar", min_value=1, max_value=30, value=30, step=1)

# --- Consulta SQL ---
query = f"SELECT * FROM produccion_petrolera LIMIT {limite}"
df = query_databricks(query)

# --- Limpieza de datos ---
if 'Fecha' in df.columns:
    df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')

# --- Mostrar tabla ---
st.subheader("Datos de producción")
st.dataframe(df)

# --- Gráfico: Producción total diaria ---
if 'Fecha' in df.columns and 'Produccion_bpd' in df.columns:
    prod_diaria = df.groupby('Fecha')['Produccion_bpd'].sum().reset_index()
    fig = px.line(prod_diaria, x='Fecha', y='Produccion_bpd',
                  title='Producción Total Diaria (barriles por día)',
                  labels={'Produccion_bpd': 'Producción (bpd)', 'Fecha': 'Fecha'})
    st.plotly_chart(fig)
else:
    st.warning("Las columnas 'Fecha' o 'Produccion_bpd' no están disponibles para graficar producción diaria.")

# --- Gráfico: Producción por pozo ---
if 'Pozo' in df.columns and 'Produccion_bpd' in df.columns:
    fig2 = px.bar(df, x='Pozo', y='Produccion_bpd', title='Producción por Pozo',
                  labels={'Produccion_bpd': 'Producción (bpd)', 'Pozo': 'Pozo'})
    st.plotly_chart(fig2)
else:
    st.warning("No se encontraron columnas 'Pozo' o 'Produccion_bpd' para graficar producción por pozo.")

# --- Sección de ayuda ---
with st.expander("❓ Acerca de la conexión y datos utilizados"):
    st.markdown(
        f"""
        ### Cómo se conectó esta app a Databricks

        - **Servidor:** `{DATABRICKS_SERVER_HOSTNAME}`
        - **HTTP Path:** `{DATABRICKS_HTTP_PATH}`  
        - **Autenticación:** Token de acceso seguro mediante `st.secrets`.

        La consulta SQL extrae datos desde la tabla `produccion_petrolera` creada en Databricks, la cual contiene datos históricos de producción petrolera.

        El dashboard muestra:  
        - Tabla de registros.  
        - Producción total diaria.  
        - Producción por pozo.

        ---
        **Herramientas usadas:**  
        Streamlit, Pandas, Plotly, Databricks SQL Connector para Python.
        """
    )

# --- Footer ---
st.markdown("---")
st.caption("Creado por Javier Horacio Pérez Ricárdez para la posición Data Engineer Jr en KodeFree.")
