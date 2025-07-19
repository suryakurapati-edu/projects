# streamlit run
import os
import json
import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2

# Always set the page config at the top of the file
st.set_page_config(page_title="OTT Analytics Dashboard", layout="wide")

# Load dashboard config file
def load_config(filename="dashboard.conf"):
    base_path = os.path.dirname(os.path.abspath(__file__))
    full_path = os.path.join(base_path, filename)
    with open(full_path, 'r') as f:
        return json.load(f)

# Cache-safe version of data loader function
@st.cache_data(ttl=600)
def load_data(_view_path, _get_connection_fn):
    conn = _get_connection_fn()
    query = f"SELECT * FROM {_view_path}"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# Cached DB connection as a static function
@st.cache_resource
def get_db_connection(_db_config):
    return psycopg2.connect(
        dbname=_db_config["dbname"],
        user=_db_config["user"],
        password=_db_config["password"],
        host=_db_config["host"],
        port=_db_config["port"]
    )

# Dashboard Class
class OTTDashboard:
    def __init__(self, config):
        self.config = config
        self.db_config = config["database"]
        self.view_path = f"{config['source_db_schema']}.{config['source_view']}"

    def run(self):
        df = load_data(self.view_path, lambda: get_db_connection(self.db_config))

        st.title("OTT Analytics Dashboard")
        st.markdown("A Data-Driven Overview of OTT Streaming Platforms")

        # Sidebar filters
        st.sidebar.header("Filters")
        platform_filter = st.sidebar.multiselect("Select Platform", df["ott_platform"].unique(), default=df["ott_platform"].unique())
        year_filter = st.sidebar.slider("Select Release Year Range", int(df["release_year"].min()), int(df["release_year"].max()), (int(df["release_year"].min()), int(df["release_year"].max())))

        df_filtered = df[(df["ott_platform"].isin(platform_filter)) & (df["release_year"].between(year_filter[0], year_filter[1]))]

        # KPI Metrics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Titles", df_filtered["total_titles"].sum())
        col2.metric("Total Movies", df_filtered["total_movies"].sum())
        col3.metric("Total TV Shows", df_filtered["total_tv_shows"].sum())
        col4.metric("Unique Countries", df_filtered["unique_countries"].sum())

        # Charts
        st.subheader("Titles per Platform")
        fig1 = px.bar(df_filtered.groupby("ott_platform")["total_titles"].sum().reset_index(), x="ott_platform", y="total_titles", color="ott_platform", title="Total Titles per Platform")
        st.plotly_chart(fig1, use_container_width=True)

        st.subheader("Movies vs TV Shows per Platform")
        df_tv_movie = df_filtered.groupby("ott_platform")[["total_movies", "total_tv_shows"]].sum().reset_index()
        fig2 = px.bar(df_tv_movie.melt(id_vars="ott_platform"), x="ott_platform", y="value", color="variable", barmode="group", title="Movies vs TV Shows")
        st.plotly_chart(fig2, use_container_width=True)

        st.subheader("Average Movie Duration per Platform")
        fig3 = px.bar(df_filtered.groupby("ott_platform")["avg_movie_duration_min"].mean().reset_index(), x="ott_platform", y="avg_movie_duration_min", color="ott_platform", title="Avg Movie Duration")
        st.plotly_chart(fig3, use_container_width=True)

        st.subheader("Monthly Content Addition Trend")
        fig4 = px.line(df_filtered.groupby("content_added_month")["total_titles"].sum().reset_index(), x="content_added_month", y="total_titles", title="Monthly Content Added Trend")
        st.plotly_chart(fig4, use_container_width=True)

        st.subheader("Unique Directors Count per Platform")
        fig5 = px.bar(df_filtered.groupby("ott_platform")["unique_directors"].sum().reset_index(), x="ott_platform", y="unique_directors", color="ott_platform", title="Unique Directors per Platform")
        st.plotly_chart(fig5, use_container_width=True)

        st.subheader("TV Shows Average Seasons per Platform")
        fig6 = px.bar(df_filtered.groupby("ott_platform")["avg_tvshow_seasons"].mean().reset_index(), x="ott_platform", y="avg_tvshow_seasons", color="ott_platform", title="Avg Seasons per TV Show")
        st.plotly_chart(fig6, use_container_width=True)

        # st.markdown("These insights help evaluate content trends, platform strategy, and user preferences for building your OTT business.")

# Main
if __name__ == "__main__":
    config = load_config("dashboard.conf")
    dashboard = OTTDashboard(config)
    dashboard.run()
