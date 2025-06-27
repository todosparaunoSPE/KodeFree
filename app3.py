# -*- coding: utf-8 -*-
"""
Created on Thu Jun 26 21:05:55 2025

@author: jahop
"""

import streamlit as st
import pandas as pd
from databricks import sql
import plotly.express as px

# --- Configuración de conexión a Databricks ---
DATABRICKS_SERVER_HOSTNAME = "dbc-8d323cd0-08cd.cloud.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/72b1345bdedbf507"
DATABRICKS_ACCESS_TOKEN = "dapi0806256d46ff204481995052856a86c9"

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

# --- Imagen o logo (puedes reemplazar por el tuyo) ---
#st.markdown("![Logo](logo.png)", unsafe_allow_html=True)

# --- Título y descripción ---
st.title("📊 Dashboard Producción Petrolera - KodeFree Data Engineer Jr")
st.markdown(
    """
    Bienvenido al dashboard interactivo de producción petrolera.  
    Aquí puedes explorar datos reales consultados desde **Databricks** y visualizar gráficos dinámicos.
    """
)


# Sidebar para filtros y controles
with st.sidebar:
    st.image("logo.png", width=150)



# --- Barra lateral: filtros ---
st.sidebar.header("Filtros")
#st.sidebar.info("💡 Consejo: cambia entre modo claro/oscuro en el menú de Streamlit (arriba a la derecha).")

limite = st.sidebar.slider("Número de registros a mostrar", min_value=1, max_value=30, value=30, step=1)

# Consulta y carga de datos
query = f"SELECT * FROM produccion_petrolera LIMIT {limite}"
df = query_databricks(query)

# Validar y preparar datos
if 'Fecha' in df.columns:
    df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')

# --- Métricas generales ---
st.subheader("📌 Estadísticas generales")
col1, col2, col3 = st.columns(3)
col1.metric("Pozos únicos", df['Pozo'].nunique() if 'Pozo' in df.columns else "N/A")
col2.metric("Total Producción", f"{df['Produccion_bpd'].sum():,.0f} bpd" if 'Produccion_bpd' in df.columns else "N/A")
col3.metric("Fechas únicas", df['Fecha'].nunique() if 'Fecha' in df.columns else "N/A")

# --- Mostrar tabla filtrada ---
st.subheader("📋 Datos de producción")
st.dataframe(df)

# --- Botón de descarga ---
st.download_button("⬇️ Descargar datos como CSV", df.to_csv(index=False), file_name="produccion_petrolera.csv")

# --- Visualización producción diaria total ---
if 'Fecha' in df.columns and 'Produccion_bpd' in df.columns:
    prod_diaria = df.groupby('Fecha')['Produccion_bpd'].sum().reset_index()
    fig = px.line(prod_diaria, x='Fecha', y='Produccion_bpd',
                  title='📈 Producción Total Diaria (barriles por día)',
                  labels={'Produccion_bpd': 'Producción (bpd)', 'Fecha': 'Fecha'})
    st.plotly_chart(fig)
else:
    st.warning("Las columnas 'Fecha' o 'Produccion_bpd' no están disponibles para graficar producción diaria.")

# --- Visualización producción por pozo ---
if 'Pozo' in df.columns and 'Produccion_bpd' in df.columns:
    fig2 = px.bar(df, x='Pozo', y='Produccion_bpd', title='🏭 Producción por Pozo',
                  labels={'Produccion_bpd': 'Producción (bpd)', 'Pozo': 'Pozo'})
    st.plotly_chart(fig2)
else:
    st.warning("No se encontraron columnas 'Pozo' o 'Produccion_bpd' para graficar producción por pozo.")

# --- Gráfico de distribución (boxplot) ---
if 'Pozo' in df.columns and 'Produccion_bpd' in df.columns:
    st.subheader("📦 Distribución de Producción por Pozo")
    fig3 = px.box(df, x='Pozo', y='Produccion_bpd', title="Distribución de producción por pozo")
    st.plotly_chart(fig3)

# --- Sección de ayuda ---
with st.expander("❓ Acerca de la conexión y datos utilizados"):
    st.markdown(
        """
        ### Cómo se conectó esta app a Databricks

        - **Servidor:** `{hostname}`
        - **HTTP Path:** `{http_path}`  
        - **Autenticación:** Token de acceso personal (Databricks Access Token).

        La consulta SQL extrae datos desde la tabla `produccion_petrolera` creada en Databricks, la cual contiene datos históricos de producción petrolera.

        El dashboard muestra:  
        - La tabla con registros cargados dinámicamente.  
        - Producción total diaria.  
        - Producción por cada pozo.  
        - Distribución de producción por pozo.

        Puedes modificar el número de registros a consultar usando el control deslizante en la barra lateral.

        ---
        **Herramientas usadas:**  
        Streamlit, Pandas, Plotly, Databricks SQL Connector para Python.
        """
        .format(hostname=DATABRICKS_SERVER_HOSTNAME, http_path=DATABRICKS_HTTP_PATH)
    )

# --- Footer ---
st.markdown("---")
st.caption("Creado por Javier Horacio Pérez Ricárdez para la posición Data Engineer Jr en KodeFree.")
