import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
from sqlalchemy import create_engine # Though not strictly used, often associated with database ops
import json
import requests
import os

# --- Streamlit Page Configuration ---
st.set_page_config(
    page_title="PhonePe Pulse Data Analysis",
    layout="wide",
    initial_sidebar_state="expanded"
)


DB_CONFIG = {
    'user': 'postgres',
    'password': 'deepika090596',
    'host': 'localhost',
    'port': '5432',
    'database': 'phonepe_data'
}

# --- Cached Database Connection ---
@st.cache_resource
def init_connection():
    """
    Establishes and caches a single PostgreSQL connection for the app.
    Uses st.cache_resource to ensure the connection is created only once
    across all user sessions.
    """
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            port=DB_CONFIG['port'],
            database=DB_CONFIG['database'],
            password=DB_CONFIG['password']
        )
        st.sidebar.success("Successfully connected to the PostgreSQL database!")
        return conn
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        st.warning("Please ensure your PostgreSQL database is running and credentials in DB_CONFIG are correct.")
        st.stop() # Stop the app if connection fails


@st.cache_data(ttl=3600)
def get_dataframes(_connection):
    """
    Fetches all nine DataFrames from the database using the cached connection.
    This function is decorated with `@st.cache_data` to cache the results,
    preventing redundant database queries on subsequent app reruns (within ttl).
    The '_connection' argument is prefixed with an underscore to prevent
    Streamlit from attempting to hash the unhashable psycopg2 connection object.
    """
    try:
        cursor = _connection.cursor()

        # Function to fetch data and create DataFrame with explicit columns
        def fetch_and_create_df(table_name, columns_tuple):
            cursor.execute(f"SELECT * FROM {table_name}")
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns_tuple)

        # Define columns for each table explicitly (ensure these match your DB schema)
        agg_trans_cols = ("States", "Years", "Quarter", "Transaction_type", "Transaction_count", "Transaction_amount")
        map_trans_cols = ("States", "Years", "Quarter", "Districts", "Transaction_count", "Transaction_amount")
        top_trans_cols = ("States", "Years", "Quarter", "Pincodes", "Transaction_count", "Transaction_amount")
        agg_user_cols = ("States", "Years", "Quarter", "Brands", "Transaction_count", "Percentage")
        map_user_cols = ("States", "Years", "Quarter", "Districts", "RegisteredUsers", "AppOpens")
        top_user_cols = ("States", "Years", "Quarter", "Pincodes", "RegisteredUsers")
        agg_insur_cols = ("States", "Years", "Quarter", "Transaction_type", "Transaction_count", "Transaction_amount")
        map_insur_cols = ("States", "Years", "Quarter", "Districts", "Transaction_count", "Transaction_amount")
        top_insur_cols = ("States", "Years", "Quarter", "Pincodes", "Transaction_count", "Transaction_amount")

        # Fetch data for each table into Pandas DataFrames
        df_aggregated_trans = fetch_and_create_df("aggregated_transaction", agg_trans_cols)
        df_map_trans = fetch_and_create_df("map_transaction", map_trans_cols)
        df_top_trans = fetch_and_create_df("top_transaction", top_trans_cols)
        df_aggregated_user = fetch_and_create_df("aggregated_user", agg_user_cols)
        df_map_user = fetch_and_create_df("map_user", map_user_cols)
        df_top_user = fetch_and_create_df("top_user", top_user_cols)
        df_aggregated_insur = fetch_and_create_df("aggregated_insurance", agg_insur_cols)
        df_map_insur = fetch_and_create_df("map_insurance", map_insur_cols)
        df_top_insur = fetch_and_create_df("top_insurance", top_insur_cols)

        cursor.close()

        # Define a mapping for common column name corrections (lowercase to TitleCase/CamelCase)
        column_name_mapping = {
            'states': 'States',
            'years': 'Years',
            'quarter': 'Quarter',
            'transaction_type': 'Transaction_type',
            'transaction_count': 'Transaction_count',
            'transaction_amount': 'Transaction_amount',
            'brands': 'Brands',
            'percentage': 'Percentage',
            'districts': 'Districts',
            'registeredusers': 'RegisteredUsers',
            'appopens': 'AppOpens',
            'pincodes': 'Pincodes',
        }

        # Apply column renaming and state cleaning for each DataFrame
        for df in [df_aggregated_trans, df_map_trans, df_top_trans,
                   df_aggregated_user, df_map_user, df_top_user,
                   df_aggregated_insur, df_map_insur, df_top_insur]:
            if not df.empty:
                df.rename(columns={col: column_name_mapping[col.lower()] for col in df.columns if col.lower() in column_name_mapping}, inplace=True)

                if 'States' in df.columns:
                    # Standardize state names for consistency with GeoJSON
                    df['States'] = df['States'].str.replace('andaman & nicobar','Andaman & Nicobar')
                    df['States'] = df['States'].str.replace("-"," ")
                    df['States'] = df['States'].str.title()
                    df['States'] = df['States'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')
                    df['States'] = df['States'].str.replace('Nct Of Delhi', 'NCT of Delhi')
                    df['States'] = df['States'].str.replace('Orissa', 'Odisha')
                    df['States'] = df['States'].str.replace('Arunanchal Pradesh', 'Arunachal Pradesh')
                    df['States'] = df['States'].str.replace('Uttar Pradesh', 'Uttar Pradesh')
                    df['States'] = df['States'].str.replace('Madhya Pradesh', 'Madhya Pradesh')
                    df['States'] = df['States'].str.replace('Jammu & Kashmir', 'Jammu and Kashmir') # Common variation
                    df['States'] = df['States'].str.replace('Puducherry', 'Puducherry') # Just ensure consistency
                    df['States'] = df['States'].str.replace('Jharkhand', 'Jharkhand') # Ensure Jharkhand is consistent


        return (df_aggregated_trans, df_map_trans, df_top_trans,
                df_aggregated_user, df_map_user, df_top_user,
                df_aggregated_insur, df_map_insur, df_top_insur)
    except Exception as e:
        st.error(f"Error fetching data from database: {e}")
        empty_df = pd.DataFrame()
        return (empty_df, empty_df, empty_df, empty_df, empty_df, empty_df, empty_df, empty_df, empty_df)

# Initialize database connection
conn = init_connection()

# Load all DataFrames using the cached connection
(
    df_aggregated_trans, df_map_trans, df_top_trans,
    df_aggregated_user, df_map_user, df_top_user,
    df_aggregated_insur, df_map_insur, df_top_insur
) = get_dataframes(conn)

# --- Sidebar Navigation ---
st.sidebar.title("Navigation")
if 'selected_scenario' not in st.session_state:
    st.session_state.selected_scenario = "Home"

scenario_options = [
    "Home",
    "1. Transaction Dynamics",
    "2. User Demographics",
    "3. Insurance Insights",
    "4. Top Transactions",
    "5. Top Users",
    "6. Top Insurance",
    "7. Explore by State (Transactions)",
    "8. Explore by State (Users)"
    
]
selected_scenario = st.sidebar.radio("Choose a Section", scenario_options, key="scenario_radio")

if selected_scenario != st.session_state.selected_scenario:
    st.session_state.selected_scenario = selected_scenario
    st.rerun()



# Home Page 
if st.session_state.selected_scenario == "Home":
    st.title(" India State-wise PhonePe Pulse Data Overview")
    st.markdown("Explore key metrics and visualizations of PhonePe usage across India.")

    st.header("Total Transaction Amount Across States")

    if not df_aggregated_trans.empty and 'Years' in df_aggregated_trans.columns:
        years = sorted(df_aggregated_trans['Years'].unique())
        quarters = sorted(df_aggregated_trans['Quarter'].unique())
        states = sorted(df_aggregated_trans['States'].unique()) # Get unique states

        col1, col2, col3 = st.columns(3)
        with col1:
            default_year_index = len(years)-1 if years else 0
            selected_year_home = st.selectbox("Select Year", years, index=default_year_index, key="home_year_map")
        with col2:
            default_quarter_index = 0 if quarters else 0
            selected_quarter_home = st.selectbox("Select Quarter", quarters, index=default_quarter_index, key="home_quarter_map")
        with col3:
            all_states_option = "All States (Overview)"
            selected_state_home = st.selectbox("Select State for Details", [all_states_option] + states, key="home_state_detail")


        if years and quarters:
            filtered_df_home = df_aggregated_trans[
                (df_aggregated_trans['Years'] == selected_year_home) &
                (df_aggregated_trans['Quarter'] == selected_quarter_home)
            ]

            state_transactions_df = filtered_df_home.groupby('States')['Transaction_amount'].sum().reset_index()
            state_transactions_df.columns = ['States', 'Total_Transaction_Amount']

            state_mapping_for_geojson = {
                'Andaman & Nicobar': 'Andaman & Nicobar Islands',
                'Dadra and Nagar Haveli and Daman and Diu': 'Dadra and Nagar Haveli and Daman & Diu',
                'NCT of Delhi': 'NCT of Delhi',
                'Odisha': 'Odisha',
                'Arunachal Pradesh': 'Arunachal Pradesh',
                'Jammu and Kashmir': 'Jammu & Kashmir',
                'Jharkhand': 'Jharkhand' 
            }
            state_transactions_df['States_GeoJSON'] = state_transactions_df['States'].replace(state_mapping_for_geojson)


            india_states_geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            try:
                @st.cache_data
                def load_geojson(url):
                    response = requests.get(url)
                    response.raise_for_status()
                    return response.json()

                india_states_geojson = load_geojson(india_states_geojson_url)
            except requests.exceptions.RequestException as e:
                st.error(f"Error loading GeoJSON data from URL: {e}")
                india_states_geojson = None
            except json.JSONDecodeError as e:
                st.error(f"Error decoding GeoJSON data: {e}. The file might be corrupted or not valid JSON.")
                india_states_geojson = None


            if india_states_geojson:
                fig_home_map = px.choropleth(
                    state_transactions_df,
                    geojson=india_states_geojson,
                    featureidkey='properties.ST_NM',
                    locations='States',
                    color='Total_Transaction_Amount',
                    color_continuous_scale='plasma',
                    hover_name='States',
                    hover_data={'Total_Transaction_Amount': ':.2s'},
                    title=f'Total Transaction Amount in India ({selected_year_home} Q{selected_quarter_home})'
                )

                fig_home_map.update_geos(
                    fitbounds="locations",
                    visible=False,
                    projection_scale=1
                )
                fig_home_map.update_layout(height=600, margin={"r":0,"t":50,"l":0,"b":0})

                st.plotly_chart(fig_home_map, use_container_width=True)
            else:
                st.warning("Could not load map data. Please check your internet connection or the GeoJSON URL.")

            
            st.subheader("Top 5 States: Current vs. Previous Year Transaction Value")
            if len(years) > 1:
                latest_year = selected_year_home
                latest_quarter = selected_quarter_home

                # Get data for the latest selected quarter
                df_latest_quarter = df_aggregated_trans[
                    (df_aggregated_trans['Years'] == latest_year) &
                    (df_aggregated_trans['Quarter'] == latest_quarter)
                ].groupby('States')['Transaction_amount'].sum().reset_index()
                df_latest_quarter.rename(columns={'Transaction_amount': f'Amount_Q{latest_quarter}_{latest_year}'}, inplace=True)

                # Get data for the same quarter in the previous year
                previous_year = latest_year - 1
                df_previous_quarter = df_aggregated_trans[
                    (df_aggregated_trans['Years'] == previous_year) &
                    (df_aggregated_trans['Quarter'] == latest_quarter)
                ].groupby('States')['Transaction_amount'].sum().reset_index()
                df_previous_quarter.rename(columns={'Transaction_amount': f'Amount_Q{latest_quarter}_{previous_year}'}, inplace=True)

                # Merge and calculate YoY growth
                merged_df = pd.merge(df_latest_quarter, df_previous_quarter, on='States', how='left').fillna(0)
                merged_df['YoY_Growth_Percentage'] = ((merged_df[f'Amount_Q{latest_quarter}_{latest_year}'] - merged_df[f'Amount_Q{latest_quarter}_{previous_year}']) / merged_df[f'Amount_Q{latest_quarter}_{previous_year}'].replace(0, pd.NA)) * 100
                
                # Sort by latest quarter's amount and display top 5
                top_5_states_yoy = merged_df.sort_values(by=f'Amount_Q{latest_quarter}_{latest_year}', ascending=False).head(5)
                st.dataframe(top_5_states_yoy.style.format({
                    f'Amount_Q{latest_quarter}_{latest_year}': "₹{:,.0f}",
                    f'Amount_Q{latest_quarter}_{previous_year}': "₹{:,.0f}",
                    'YoY_Growth_Percentage': "{:.2f}%"
                }))
            else:
                st.info("Not enough historical data (less than 2 years) to show Year-over-Year comparison.")

            
            st.subheader(f" Top Transaction Types Across India ({selected_year_home} Q{selected_quarter_home})")
            if not filtered_df_home.empty:
                transaction_type_summary = filtered_df_home.groupby('Transaction_type').agg(
                    Total_Amount=('Transaction_amount', 'sum'),
                    Total_Count=('Transaction_count', 'sum')
                ).reset_index().sort_values(by='Total_Amount', ascending=False)
                
                st.dataframe(transaction_type_summary.style.format({
                    'Total_Amount': "₹{:,.0f}",
                    'Total_Count': "{:,.0f}"
                }))
            else:
                st.info("No transaction type data available for the selected period.")


            if selected_state_home != all_states_option:
                st.subheader(f"Transaction Details for {selected_state_home} ({selected_year_home} Q{selected_quarter_home})")
                state_detail_df = filtered_df_home[filtered_df_home['States'] == selected_state_home]

                if not state_detail_df.empty:
                    state_type_summary = state_detail_df.groupby('Transaction_type').agg(
                        Count=('Transaction_count', 'sum'),
                        Amount=('Transaction_amount', 'sum')
                    ).reset_index().sort_values(by='Amount', ascending=False)

                    st.write(f"#### Transaction Breakdown by Type in {selected_state_home}")
                    st.dataframe(state_type_summary, use_container_width=True)

                    fig_state_type_amount = px.bar(
                        state_type_summary,
                        x='Transaction_type',
                        y='Amount',
                        title=f'Transaction Amount by Type in {selected_state_home}',
                        labels={'Amount': 'Total Amount (INR)'},
                        template='plotly_white'
                    )
                    st.plotly_chart(fig_state_type_amount, use_container_width=True)

                else:
                    st.info(f"No detailed transaction data available for {selected_state_home} in {selected_year_home} Q{selected_quarter_home}.")
        else:
            st.info("No years or quarters found in transaction data for map. Cannot display map filters.")
    else:
        st.info("No transaction data available for mapping. Please check database connection.")


# 1. Decoding Transaction Dynamics on PhonePe
elif st.session_state.selected_scenario == "1. Transaction Dynamics":
    st.title(" Decoding Transaction Dynamics on PhonePe")
    st.markdown("This section provides insights into transaction patterns across states, quarters, and payment categories.")

    if not df_aggregated_trans.empty and 'Years' in df_aggregated_trans.columns:
        years = sorted(df_aggregated_trans['Years'].unique())
        quarters = sorted(df_aggregated_trans['Quarter'].unique())

        col1, col2 = st.columns(2)
        with col1:
            selected_year = st.selectbox("Select Year", years, key="agg_trans_year")
        with col2:
            selected_quarter = st.selectbox("Select Quarter", quarters, key="agg_trans_quarter")

        filtered_df = df_aggregated_trans[
            (df_aggregated_trans['Years'] == selected_year) &
            (df_aggregated_trans['Quarter'] == selected_quarter)
        ]

        if not filtered_df.empty:
            st.subheader(f"Transaction Trends for {selected_year} - Q{selected_quarter}")

            st.write("#### Transaction Count by Type")
            fig_count = px.bar(
                filtered_df,
                x='Transaction_type',
                y='Transaction_count',
                color='Transaction_type',
                title='Transaction Count by Type',
                labels={'Transaction_count': 'Count', 'Transaction_type': 'Type'},
                template='plotly_white'
            )
            st.plotly_chart(fig_count, use_container_width=True)

            st.write("#### Transaction Amount by Type")
            fig_amount = px.bar(
                filtered_df,
                x='Transaction_type',
                y='Transaction_amount',
                color='Transaction_type',
                title='Total Transaction Amount by Type',
                labels={'Transaction_amount': 'Amount (INR)', 'Transaction_type': 'Type'},
                template='plotly_white'
            )
            st.plotly_chart(fig_amount, use_container_width=True)

            st.write("#### State-wise Transaction Summary")
            state_summary = filtered_df.groupby('States').agg(
                Total_Count=('Transaction_count', 'sum'),
                Total_Amount=('Transaction_amount', 'sum')
            ).reset_index()
            st.dataframe(state_summary.sort_values(by='Total_Amount', ascending=False))

            # --- Visualization for State-wise Transaction Summary (Total Amount) ---
            st.write("#### State-wise Transaction Amount Overview")
            fig_state_amount = px.bar(
                state_summary.sort_values(by='Total_Amount', ascending=False),
                x='States',
                y='Total_Amount',
                title=f'Total Transaction Amount by State ({selected_year} Q{selected_quarter})',
                labels={'Total_Amount': 'Total Amount (INR)', 'States': 'State'},
                template='plotly_white'
            )
            st.plotly_chart(fig_state_amount, use_container_width=True)

            # --- Visualization for State-wise Transaction Summary (Total Count) 
            st.write("#### State-wise Transaction Count Overview")
            fig_state_count = px.bar(
                state_summary.sort_values(by='Total_Count', ascending=False),
                x='States',
                y='Total_Count',
                title=f'Total Transaction Count by State ({selected_year} Q{selected_quarter})',
                labels={'Total_Count': 'Total Count', 'States': 'State'},
                template='plotly_white'
            )
            st.plotly_chart(fig_state_count, use_container_width=True)


            # --- Business Case Study: Market Share of Transaction Types ---
            st.subheader(f" Market Share of Transaction Types ({selected_year} Q{selected_quarter})")
            total_count_all = filtered_df['Transaction_count'].sum()
            total_amount_all = filtered_df['Transaction_amount'].sum()

            market_share_df = filtered_df.groupby('Transaction_type').agg(
                Total_Count=('Transaction_count', 'sum'),
                Total_Amount=('Transaction_amount', 'sum')
            ).reset_index()

            if total_count_all > 0:
                market_share_df['Count_Share_Percentage'] = (market_share_df['Total_Count'] * 100.0 / total_count_all)
            else:
                market_share_df['Count_Share_Percentage'] = 0

            if total_amount_all > 0:
                market_share_df['Amount_Share_Percentage'] = (market_share_df['Total_Amount'] * 100.0 / total_amount_all)
            else:
                market_share_df['Amount_Share_Percentage'] = 0

            st.dataframe(market_share_df.sort_values(by='Amount_Share_Percentage', ascending=False).style.format({
                'Total_Count': "{:,.0f}",
                'Total_Amount': "₹{:,.0f}",
                'Count_Share_Percentage': "{:.2f}%",
                'Amount_Share_Percentage': "{:.2f}%"
            }))

            
            st.subheader(f"💲 Average Transaction Value by Type ({selected_year} Q{selected_quarter})")
            avg_trans_value_df = filtered_df[filtered_df['Transaction_count'] > 0].copy()
            avg_trans_value_df['Average_Value'] = avg_trans_value_df['Transaction_amount'] / avg_trans_value_df['Transaction_count']
            
            avg_trans_summary = avg_trans_value_df.groupby('Transaction_type').agg(
                Avg_Transaction_Value=('Average_Value', 'mean'),
                Total_Count=('Transaction_count', 'sum')
            ).reset_index().sort_values(by='Avg_Transaction_Value', ascending=False)

            st.dataframe(avg_trans_summary.style.format({
                'Avg_Transaction_Value': "₹{:,.2f}",
                'Total_Count': "{:,.0f}"
            }))

        else:
            st.info("No transaction data available for the selected year and quarter.")
    else:
        st.warning("Transaction data (aggregated_transaction) not loaded or 'Years' column missing. Please check database connection and table schema.")


# 2. User Demographics
elif st.session_state.selected_scenario == "2. User Demographics":
    st.title(" User Demographics on PhonePe")
    st.markdown("This section analyzes user distribution, app usage, and brand preferences.")

    if not df_aggregated_user.empty and not df_map_user.empty and 'Years' in df_aggregated_user.columns:
        years = sorted(df_aggregated_user['Years'].unique())
        quarters = sorted(df_aggregated_user['Quarter'].unique())
        states = sorted(df_aggregated_user['States'].unique()) # Get states for brand analysis

        col1, col2, col3 = st.columns(3)
        with col1:
            selected_year_user = st.selectbox("Select Year", years, key="user_demog_year")
        with col2:
            selected_quarter_user = st.selectbox("Select Quarter", quarters, key="user_demog_quarter")
        with col3:
            selected_state_user_brand = st.selectbox("Select State for Brand Analysis", states, key="user_brand_state")

        filtered_agg_user = df_aggregated_user[
            (df_aggregated_user['Years'] == selected_year_user) &
            (df_aggregated_user['Quarter'] == selected_quarter_user)
        ]

        if not filtered_agg_user.empty:
            st.subheader(f"User App Brands Distribution ({selected_year_user} Q{selected_quarter_user})")
            fig_brands = px.pie(
                filtered_agg_user,
                values='Transaction_count',
                names='Brands',
                title='User Count by Mobile Brand',
                hole=0.4
            )
            st.plotly_chart(fig_brands, use_container_width=True)

        filtered_map_user = df_map_user[
            (df_map_user['Years'] == selected_year_user) &
            (df_map_user['Quarter'] == selected_quarter_user)
        ]

        if not filtered_map_user.empty:
            st.subheader(f"Registered Users & App Opens by District ({selected_year_user} Q{selected_quarter_user})")
            top_districts_users = filtered_map_user.groupby('Districts').agg(
                Total_Registered_Users=('RegisteredUsers', 'sum'),
                Total_App_Opens=('AppOpens', 'sum')
            ).reset_index().sort_values(by='Total_Registered_Users', ascending=False).head(20)

            st.dataframe(top_districts_users)

            fig_top_districts = px.bar(
                top_districts_users,
                x='Districts',
                y='Total_Registered_Users',
                title='Top 20 Districts by Registered Users',
                labels={'Total_Registered_Users': 'Registered Users'},
                template='plotly_white'
            )
            st.plotly_chart(fig_top_districts, use_container_width=True)
        else:
            st.info("No user data available for the selected year and quarter.")

        
        st.subheader(f" Mobile Brand User Base Trends in {selected_state_user_brand}")
        if not df_aggregated_user.empty:
            brand_trend_df = df_aggregated_user[
                df_aggregated_user['States'] == selected_state_user_brand
            ].groupby(['Years', 'Brands'])['Transaction_count'].sum().unstack(fill_value=0)

            if not brand_trend_df.empty:
                st.dataframe(brand_trend_df.style.format("{:,.0f}"))
                
                fig_brand_trend = px.line(
                    brand_trend_df,
                    x=brand_trend_df.index,
                    y=brand_trend_df.columns,
                    title=f'User Count by Mobile Brand Over Years in {selected_state_user_brand}',
                    labels={'value': 'Total Users', 'Years': 'Year', 'variable': 'Brand'},
                    template='plotly_white'
                )
                fig_brand_trend.update_layout(hovermode="x unified")
                st.plotly_chart(fig_brand_trend, use_container_width=True)
            else:
                st.info(f"No mobile brand data available for {selected_state_user_brand}.")
        else:
            st.warning("User data (aggregated_user) not loaded. Cannot analyze brand trends.")

        
        st.subheader(f" Total Users & App Opens Over Time in {selected_state_user_brand}")
        if not df_map_user.empty:
            user_time_series_df = df_map_user[
                df_map_user['States'] == selected_state_user_brand
            ].groupby(['Years', 'Quarter']).agg(
                Total_Registered_Users=('RegisteredUsers', 'sum'),
                Total_App_Opens=('AppOpens', 'sum')
            ).reset_index()

            if not user_time_series_df.empty:
                user_time_series_df['Period'] = user_time_series_df['Years'].astype(str) + ' Q' + user_time_series_df['Quarter'].astype(str)
                
                fig_user_time_series = go.Figure()
                fig_user_time_series.add_trace(go.Scatter(x=user_time_series_df['Period'], y=user_time_series_df['Total_Registered_Users'], mode='lines+markers', name='Registered Users'))
                fig_user_time_series.add_trace(go.Scatter(x=user_time_series_df['Period'], y=user_time_series_df['Total_App_Opens'], mode='lines+markers', name='App Opens'))
                
                fig_user_time_series.update_layout(
                    title=f'Registered Users and App Opens Over Time in {selected_state_user_brand}',
                    xaxis_title='Year and Quarter',
                    yaxis_title='Count',
                    template='plotly_white'
                )
                st.plotly_chart(fig_user_time_series, use_container_width=True)
            else:
                st.info(f"No time-series user data available for {selected_state_user_brand}.")
        else:
            st.warning("Mapped user data (map_user) not loaded. Cannot analyze time-series user data.")

    else:
        st.warning("User data (aggregated_user or map_user) not loaded or 'Years' column missing. Please check database connection and table schema.")


# 3. Insurance Insights
elif st.session_state.selected_scenario == "3. Insurance Insights":
    st.title(" Insurance Insights on PhonePe")
    st.markdown("Dive into transaction patterns and user engagement related to insurance products.")

    if not df_aggregated_insur.empty and not df_map_insur.empty and 'Years' in df_aggregated_insur.columns:
        years = sorted(df_aggregated_insur['Years'].unique())
        quarters = sorted(df_aggregated_insur['Quarter'].unique())

        col1, col2 = st.columns(2)
        with col1:
            selected_year_insur = st.selectbox("Select Year", years, key="insur_insights_year")
        with col2:
            selected_quarter_insur = st.selectbox("Select Quarter", quarters, key="insur_insights_quarter")

        filtered_agg_insur = df_aggregated_insur[
            (df_aggregated_insur['Years'] == selected_year_insur) &
            (df_aggregated_insur['Quarter'] == selected_quarter_insur)
        ]

        if not filtered_agg_insur.empty:
            st.subheader(f"Insurance Transaction Overview ({selected_year_insur} Q{selected_quarter_insur})")

            fig_insur_type = px.pie(
                filtered_agg_insur,
                values='Transaction_count',
                names='Transaction_type',
                title='Insurance Transaction Count by Type',
                hole=0.4
            )
            st.plotly_chart(fig_insur_type, use_container_width=True)

            fig_insur_amount = px.bar(
                filtered_agg_insur,
                x='Transaction_type',
                y='Transaction_amount',
                title='Total Insurance Transaction Amount by Type',
                labels={'Transaction_amount': 'Amount (INR)', 'Transaction_type': 'Type'},
                template='plotly_white'
            )
            st.plotly_chart(fig_insur_amount, use_container_width=True)

            state_insur_summary = filtered_agg_insur.groupby('States').agg(
                Total_Insurance_Count=('Transaction_count', 'sum'),
                Total_Insurance_Amount=('Transaction_amount', 'sum')
            ).reset_index()
            st.write("#### State-wise Insurance Summary")
            st.dataframe(state_insur_summary)

            # --- Visualization for State-wise Insurance Summary (Total Amount) ---
            st.write("#### State-wise Insurance Amount Overview")
            fig_state_insur_amount = px.bar(
                state_insur_summary.sort_values(by='Total_Insurance_Amount', ascending=False),
                x='States',
                y='Total_Insurance_Amount',
                title=f'Total Insurance Amount by State ({selected_year_insur} Q{selected_quarter_insur})',
                labels={'Total_Insurance_Amount': 'Total Amount (INR)', 'States': 'State'},
                template='plotly_white'
            )
            st.plotly_chart(fig_state_insur_amount, use_container_width=True)

            # --- Visualization for State-wise Insurance Summary (Total Count) ---
            st.write("#### State-wise Insurance Count Overview")
            fig_state_insur_count = px.bar(
                state_insur_summary.sort_values(by='Total_Insurance_Count', ascending=False),
                x='States',
                y='Total_Insurance_Count',
                title=f'Total Insurance Count by State ({selected_year_insur} Q{selected_quarter_insur})',
                labels={'Total_Insurance_Count': 'Total Count', 'States': 'State'},
                template='plotly_white'
            )
            st.plotly_chart(fig_state_insur_count, use_container_width=True)

            
            st.subheader(f" Average Premium Value by Insurance Type ({selected_year_insur} Q{selected_quarter_insur})")
            avg_premium_df = filtered_agg_insur[filtered_agg_insur['Transaction_count'] > 0].copy()
            avg_premium_df['Average_Premium_Value'] = avg_premium_df['Transaction_amount'] / avg_premium_df['Transaction_count']

            avg_premium_summary = avg_premium_df.groupby('Transaction_type').agg(
                Average_Premium_Value=('Average_Premium_Value', 'mean'),
                Total_Policies_Sold=('Transaction_count', 'sum')
            ).reset_index().sort_values(by='Average_Premium_Value', ascending=False)

            st.dataframe(avg_premium_summary.style.format({
                'Average_Premium_Value': "₹{:,.2f}",
                'Total_Policies_Sold': "{:,.0f}"
            }))

            # --- New Business Case Study: Year-over-Year Growth of Total Insurance Policies ---
            st.subheader(f" Year-over-Year Growth of Total Insurance Policies")
            if len(years) > 1:
                insurance_yoy_df = df_aggregated_insur.groupby(['Years', 'Quarter']).agg(
                    Total_Policies=('Transaction_count', 'sum')
                ).reset_index()
                insurance_yoy_df.sort_values(by=['Years', 'Quarter'], inplace=True)
                
                insurance_yoy_df['Previous_Year_Policies'] = insurance_yoy_df.groupby('Quarter')['Total_Policies'].shift(1).fillna(0)
                insurance_yoy_df['YoY_Growth_Percentage'] = ((insurance_yoy_df['Total_Policies'] - insurance_yoy_df['Previous_Year_Policies']) / insurance_yoy_df['Previous_Year_Policies'].replace(0, pd.NA)) * 100
                
                # Filter to show only the selected year and quarter's YoY growth
                current_insur_yoy = insurance_yoy_df[
                    (insurance_yoy_df['Years'] == selected_year_insur) &
                    (insurance_yoy_df['Quarter'] == selected_quarter_insur)
                ]
                
                st.dataframe(current_insur_yoy.style.format({
                    'Total_Policies': "{:,.0f}",
                    'Previous_Year_Policies': "{:,.0f}",
                    'YoY_Growth_Percentage': "{:.2f}%"
                }))
            else:
                st.info("Not enough historical data (less than 2 years) to show Year-over-Year insurance growth.")

        else:
            st.info("No insurance data available for the selected year and quarter.")
    else:
        st.warning("Insurance data (aggregated_insurance or map_insurance) not loaded or 'Years' column missing. Please check database connection and table schema.")


# 4. Top Transactions (by Pincode)
elif st.session_state.selected_scenario == "4. Top Transactions":
    st.title(" Top Transactions by Pincode")
    st.markdown("Discover the top pincodes by transaction count and amount.")

    if not df_top_trans.empty and 'Years' in df_top_trans.columns:
        years = sorted(df_top_trans['Years'].unique())
        quarters = sorted(df_top_trans['Quarter'].unique())
        states = sorted(df_top_trans['States'].unique())

        col1, col2, col3 = st.columns(3)
        with col1:
            selected_year_top_trans = st.selectbox("Select Year", years, key="top_trans_year")
        with col2:
            selected_quarter_top_trans = st.selectbox("Select Quarter", quarters, key="top_trans_quarter")
        with col3:
            selected_state_top_trans = st.selectbox("Select State", states, key="top_trans_state")

        filtered_df_top_trans = df_top_trans[
            (df_top_trans['Years'] == selected_year_top_trans) &
            (df_top_trans['Quarter'] == selected_quarter_top_trans) &
            (df_top_trans['States'] == selected_state_top_trans)
        ].sort_values(by='Transaction_amount', ascending=False)

        if not filtered_df_top_trans.empty:
            st.subheader(f"Top Pincodes for Transactions in {selected_state_top_trans} ({selected_year_top_trans} Q{selected_quarter_top_trans})")
            st.dataframe(filtered_df_top_trans.head(10))

            fig_top_trans_pincode = px.bar(
                filtered_df_top_trans.head(10),
                x='Pincodes',
                y='Transaction_amount',
                title=f'Top 10 Pincodes by Transaction Amount in {selected_state_top_trans}',
                labels={'Transaction_amount': 'Amount (INR)'},
                template='plotly_white'
            )
            st.plotly_chart(fig_top_trans_pincode, use_container_width=True)

            
            st.subheader(f" Top 10 Pincodes by Average Transaction Value in {selected_state_top_trans}")
            avg_trans_pincode_df = filtered_df_top_trans[filtered_df_top_trans['Transaction_count'] > 0].copy()
            avg_trans_pincode_df['Average_Transaction_Value'] = avg_trans_pincode_df['Transaction_amount'] / avg_trans_pincode_df['Transaction_count']

            top_avg_pincodes = avg_trans_pincode_df.sort_values(by='Average_Transaction_Value', ascending=False).head(10)

            st.dataframe(top_avg_pincodes[['Pincodes', 'Average_Transaction_Value', 'Transaction_count']].style.format({
                'Average_Transaction_Value': "₹{:,.2f}",
                'Transaction_count': "{:,.0f}"
            }))

            
            st.write("#### Top Pincodes by Average Transaction Value")
            fig_top_avg_pincodes = px.bar(
                top_avg_pincodes,
                x='Pincodes',
                y='Average_Transaction_Value',
                title=f'Top 10 Pincodes by Average Transaction Value in {selected_state_top_trans}',
                labels={'Average_Transaction_Value': 'Average Value (INR)'},
                template='plotly_white'
            )
            st.plotly_chart(fig_top_avg_pincodes, use_container_width=True)


            
            st.subheader(f" Top 10 Districts by Transaction Count in {selected_state_top_trans}")
            if not df_map_trans.empty:
                filtered_map_trans_for_district = df_map_trans[
                    (df_map_trans['States'] == selected_state_top_trans) &
                    (df_map_trans['Years'] == selected_year_top_trans) &
                    (df_map_trans['Quarter'] == selected_quarter_top_trans)
                ].copy()
                
                top_districts_by_count = filtered_map_trans_for_district.groupby('Districts')['Transaction_count'].sum().reset_index().sort_values(by='Transaction_count', ascending=False).head(10)
                
                st.dataframe(top_districts_by_count.style.format({
                    'Transaction_count': "{:,.0f}"
                }))

                # --- Visualization for Top Districts by Transaction Count ---
                st.write("#### Top Districts by Transaction Count")
                fig_top_districts_by_count = px.bar(
                    top_districts_by_count,
                    x='Districts',
                    y='Transaction_count',
                    title=f'Top 10 Districts by Transaction Count in {selected_state_top_trans}',
                    labels={'Transaction_count': 'Total Count'},
                    template='plotly_white'
                )
                st.plotly_chart(fig_top_districts_by_count, use_container_width=True)

            else:
                st.info("Mapped transaction data (map_transaction) not loaded. Cannot display top districts by count.")

        else:
            st.info("No top transaction data available for the selected criteria.")
    else:
        st.warning("Top transaction data (top_transaction) not loaded or 'Years' column missing. Please check database connection and table schema.")

# 5. Top Users (by Pincode)
elif st.session_state.selected_scenario == "5. Top Users":
    st.title(" Top Users by Pincode")
    st.markdown("Identify pincodes with the highest number of registered users.")

    if not df_top_user.empty and 'Years' in df_top_user.columns:
        years = sorted(df_top_user['Years'].unique())
        quarters = sorted(df_top_user['Quarter'].unique())
        states = sorted(df_top_user['States'].unique())

        col1, col2, col3 = st.columns(3)
        with col1:
            selected_year_top_user = st.selectbox("Select Year", years, key="top_user_year")
        with col2:
            selected_quarter_top_user = st.selectbox("Select Quarter", quarters, key="top_user_quarter")
        with col3:
            selected_state_top_user = st.selectbox("Select State", states, key="top_user_state")

        filtered_df_top_user = df_top_user[
            (df_top_user['Years'] == selected_year_top_user) &
            (df_top_user['Quarter'] == selected_quarter_top_user) &
            (df_top_user['States'] == selected_state_top_user)
        ].sort_values(by='RegisteredUsers', ascending=False)

        if not filtered_df_top_user.empty:
            st.subheader(f"Top Pincodes for Registered Users in {selected_state_top_user} ({selected_year_top_user} Q{selected_quarter_top_user})")
            st.dataframe(filtered_df_top_user.head(10))

            fig_top_user_pincode = px.bar(
                filtered_df_top_user.head(10),
                x='Pincodes',
                y='RegisteredUsers',
                title=f'Top 10 Pincodes by Registered Users in {selected_state_top_user}',
                labels={'RegisteredUsers': 'Number of Registered Users'},
                template='plotly_white'
            )
            st.plotly_chart(fig_top_user_pincode, use_container_width=True)

            
            st.subheader(f" Top 10 Districts by App Open Rate in {selected_state_top_user} ({selected_year_top_user})")
            if not df_map_user.empty:
                filtered_map_user_for_ao = df_map_user[
                    (df_map_user['Years'] == selected_year_top_user) &
                    (df_map_user['States'] == selected_state_top_user)
                ].copy()

                app_open_rate_df = filtered_map_user_for_ao.groupby('Districts').agg(
                    Total_Registered_Users=('RegisteredUsers', 'sum'),
                    Total_App_Opens=('AppOpens', 'sum')
                ).reset_index()

                app_open_rate_df['App_Open_Rate_Percentage'] = (app_open_rate_df['Total_App_Opens'] * 100.0 / app_open_rate_df['Total_Registered_Users'].replace(0, pd.NA))
                
                top_app_open_districts = app_open_rate_df.sort_values(by='App_Open_Rate_Percentage', ascending=False).head(10)

                st.dataframe(top_app_open_districts.style.format({
                    'Total_Registered_Users': "{:,.0f}",
                    'Total_App_Opens': "{:,.0f}",
                    'App_Open_Rate_Percentage': "{:.2f}%"
                }))

                # --- Visualization for Top Districts by App Open Rate ---
                st.write("#### Top Districts by App Open Rate")
                fig_top_app_open_districts = px.bar(
                    top_app_open_districts,
                    x='Districts',
                    y='App_Open_Rate_Percentage',
                    title=f'Top 10 Districts by App Open Rate in {selected_state_top_user}',
                    labels={'App_Open_Rate_Percentage': 'App Open Rate (%)'},
                    template='plotly_white'
                )
                st.plotly_chart(fig_top_app_open_districts, use_container_width=True)

            else:
                st.info("Mapped user data (map_user) not loaded. Cannot calculate app open rates.")

            
            st.subheader(f" Top 10 Districts by App Opens in {selected_state_top_user}")
            if not df_map_user.empty:
                filtered_map_user_for_app_opens = df_map_user[
                    (df_map_user['States'] == selected_state_top_user) &
                    (df_map_user['Years'] == selected_year_top_user) &
                    (df_map_user['Quarter'] == selected_quarter_top_user)
                ].copy()
                
                top_districts_by_app_opens = filtered_map_user_for_app_opens.groupby('Districts')['AppOpens'].sum().reset_index().sort_values(by='AppOpens', ascending=False).head(10)
                
                st.dataframe(top_districts_by_app_opens.style.format({
                    'AppOpens': "{:,.0f}"
                }))

                # --- Visualization for Top Districts by App Opens ---
                st.write("#### Top Districts by App Opens")
                fig_top_districts_by_app_opens = px.bar(
                    top_districts_by_app_opens,
                    x='Districts',
                    y='AppOpens',
                    title=f'Top 10 Districts by App Opens in {selected_state_top_user}',
                    labels={'AppOpens': 'Total App Opens'},
                    template='plotly_white'
                )
                st.plotly_chart(fig_top_districts_by_app_opens, use_container_width=True)

            else:
                st.info("Mapped user data (map_user) not loaded. Cannot display top districts by app opens.")


        else:
            st.info("No top user data available for the selected criteria.")
    else:
        st.warning("Top user data (top_user) not loaded or 'Years' column missing. Please check database connection and table schema.")

# 6. Top Insurance (by Pincode)
elif st.session_state.selected_scenario == "6. Top Insurance":
    st.title(" Top Insurance by Pincode")
    st.markdown("Examine pincodes with the highest insurance transaction activity.")

    if not df_top_insur.empty and 'Years' in df_top_insur.columns:
        years = sorted(df_top_insur['Years'].unique())
        quarters = sorted(df_top_insur['Quarter'].unique())
        states = sorted(df_top_insur['States'].unique())

        col1, col2, col3 = st.columns(3)
        with col1:
            selected_year_top_insur = st.selectbox("Select Year", years, key="top_insur_year")
        with col2:
            selected_quarter_top_insur = st.selectbox("Select Quarter", quarters, key="top_insur_quarter")
        with col3:
            selected_state_top_insur = st.selectbox("Select State", states, key="top_insur_state")

        filtered_df_top_insur = df_top_insur[
            (df_top_insur['Years'] == selected_year_top_insur) &
            (df_top_insur['Quarter'] == selected_quarter_top_insur) &
            (df_top_insur['States'] == selected_state_top_insur)
        ].sort_values(by='Transaction_amount', ascending=False)

        if not filtered_df_top_insur.empty:
            st.subheader(f"Top Pincodes for Insurance Transactions in {selected_state_top_insur} ({selected_year_top_insur} Q{selected_quarter_top_insur})")
            st.dataframe(filtered_df_top_insur.head(10))

            fig_top_insur_pincode = px.bar(
                filtered_df_top_insur.head(10),
                x='Pincodes',
                y='Transaction_amount',
                title=f'Top 10 Pincodes by Insurance Transaction Amount in {selected_state_top_insur}',
                labels={'Transaction_amount': 'Amount (INR)'},
                template='plotly_white'
            )
            st.plotly_chart(fig_top_insur_pincode, use_container_width=True)

            
            st.subheader(f" Top 10 Pincodes by Insurance Transaction Count in {selected_state_top_insur}")
            top_count_insur_pincodes = filtered_df_top_insur.sort_values(by='Transaction_count', ascending=False).head(10)
            st.dataframe(top_count_insur_pincodes[['Pincodes', 'Transaction_count', 'Transaction_amount']].style.format({
                'Transaction_count': "{:,.0f}",
                'Transaction_amount': "₹{:,.0f}"
            }))

            #  Visualization for Top Pincodes by Insurance Transaction Count 
            st.write("#### Top Pincodes by Insurance Transaction Count")
            fig_top_count_insur_pincodes = px.bar(
                top_count_insur_pincodes,
                x='Pincodes',
                y='Transaction_count',
                title=f'Top 10 Pincodes by Insurance Transaction Count in {selected_state_top_insur}',
                labels={'Transaction_count': 'Total Count'},
                template='plotly_white'
            )
            st.plotly_chart(fig_top_count_insur_pincodes, use_container_width=True)


            
            st.subheader(f" Top 10 Districts by Total Insurance Policies in {selected_state_top_insur}")
            if not df_map_insur.empty:
                filtered_map_insur_for_district = df_map_insur[
                    (df_map_insur['States'] == selected_state_top_insur) &
                    (df_map_insur['Years'] == selected_year_top_insur) &
                    (df_map_insur['Quarter'] == selected_quarter_top_insur)
                ].copy()
                
                top_districts_by_insur_count = filtered_map_insur_for_district.groupby('Districts')['Transaction_count'].sum().reset_index().sort_values(by='Transaction_count', ascending=False).head(10)
                
                st.dataframe(top_districts_by_insur_count.style.format({
                    'Transaction_count': "{:,.0f}"
                }))

                # --- Visualization for Top Districts by Total Insurance Policies ---
                st.write("#### Top Districts by Total Insurance Policies")
                fig_top_districts_by_insur_count = px.bar(
                    top_districts_by_insur_count,
                    x='Districts',
                    y='Transaction_count',
                    title=f'Top 10 Districts by Total Insurance Policies in {selected_state_top_insur}',
                    labels={'Transaction_count': 'Total Policies'},
                    template='plotly_white'
                )
                st.plotly_chart(fig_top_districts_by_insur_count, use_container_width=True)

            else:
                st.info("Mapped insurance data (map_insurance) not loaded. Cannot display top districts by insurance policies.")


        else:
            st.info("No top insurance data available for the selected criteria.")
    else:
        st.warning("Top insurance data (top_insurance) not loaded or 'Years' column missing. Please check database connection and table schema.")


# 7. Explore by State (Transactions)
elif st.session_state.selected_scenario == "7. Explore by State (Transactions)":
    st.title(" Explore Transactions by State")
    st.markdown("Deep-dive into transaction data for a selected state across different quarters and districts.")

    if not df_map_trans.empty and 'Years' in df_map_trans.columns:
        years = sorted(df_map_trans['Years'].unique())
        quarters = sorted(df_map_trans['Quarter'].unique())
        states = sorted(df_map_trans['States'].unique())

        col1, col2, col3 = st.columns(3)
        with col1:
            selected_year_state_trans = st.selectbox("Select Year", years, key="state_trans_year")
        with col2:
            selected_quarter_state_trans = st.selectbox("Select Quarter", quarters, key="state_trans_quarter")
        with col3:
            selected_state_state_trans = st.selectbox("Select State", states, key="state_trans_state")

        filtered_df_state_trans = df_map_trans[
            (df_map_trans['Years'] == selected_year_state_trans) &
            (df_map_trans['Quarter'] == selected_quarter_state_trans) &
            (df_map_trans['States'] == selected_state_state_trans)
        ].sort_values(by='Transaction_amount', ascending=False)

        if not filtered_df_state_trans.empty:
            st.subheader(f"Transaction Data for {selected_state_state_trans} ({selected_year_state_trans} Q{selected_quarter_state_trans})")
            st.dataframe(filtered_df_state_trans)

            fig_state_trans_dist = px.bar(
                filtered_df_state_trans,
                x='Districts',
                y='Transaction_amount',
                title=f'Transaction Amount by District in {selected_state_state_trans}',
                labels={'Transaction_amount': 'Amount (INR)'},
                template='plotly_white'
            )
            st.plotly_chart(fig_state_trans_dist, use_container_width=True)

            
            st.subheader(f" Top 5 Districts by Transaction Amount with QoQ Growth in {selected_state_state_trans}")
            
            # Prepare data for QoQ calculation
            district_quarterly_amount = df_map_trans[
                df_map_trans['States'] == selected_state_state_trans
            ].groupby(['Districts', 'Years', 'Quarter'])['Transaction_amount'].sum().reset_index()

            # Sort for correct LAG calculation
            district_quarterly_amount.sort_values(by=['Districts', 'Years', 'Quarter'], inplace=True)

            # Calculate Previous Quarter Amount and QoQ Growth
            district_quarterly_amount['Previous_Quarter_Amount'] = district_quarterly_amount.groupby('Districts')['Transaction_amount'].shift(1).fillna(0)
            district_quarterly_amount['QoQ_Growth_Percentage'] = ((district_quarterly_amount['Transaction_amount'] - district_quarterly_amount['Previous_Quarter_Amount']) / district_quarterly_amount['Previous_Quarter_Amount'].replace(0, pd.NA)) * 100

            # Filter for the selected year and quarter
            current_quarter_growth = district_quarterly_amount[
                (district_quarterly_amount['Years'] == selected_year_state_trans) &
                (district_quarterly_amount['Quarter'] == selected_quarter_state_trans)
            ]
            
            # Sort by current quarter's amount and display top 5
            top_5_districts_qoq = current_quarter_growth.sort_values(by='Transaction_amount', ascending=False).head(5)

            st.dataframe(top_5_districts_qoq.style.format({
                'Transaction_amount': "₹{:,.0f}",
                'Previous_Quarter_Amount': "₹{:,.0f}",
                'QoQ_Growth_Percentage': "{:.2f}%"
            }))

            # --- Visualization for Top 5 Districts by Transaction Amount and QoQ Growth ---
            st.write("#### Top 5 Districts by Transaction Amount with QoQ Growth")
            fig_top_5_districts_qoq = px.bar(
                top_5_districts_qoq,
                x='Districts',
                y='Transaction_amount',
                color='QoQ_Growth_Percentage', # Color by growth percentage
                color_continuous_scale='RdYlGn', # Red-Yellow-Green for growth
                title=f'Top 5 Districts by Transaction Amount and QoQ Growth in {selected_state_state_trans}',
                labels={'Transaction_amount': 'Total Amount (INR)', 'QoQ_Growth_Percentage': 'QoQ Growth (%)'},
                template='plotly_white'
            )
            st.plotly_chart(fig_top_5_districts_qoq, use_container_width=True)

            
            st.subheader(f" Transaction Count by District in {selected_state_state_trans}")
            district_trans_count = filtered_df_state_trans.groupby('Districts')['Transaction_count'].sum().reset_index().sort_values(by='Transaction_count', ascending=False)
            
            st.dataframe(district_trans_count.style.format({
                'Transaction_count': "{:,.0f}"
            }))

            # --- Visualization for Transaction Count by District ---
            st.write("#### Transaction Count by District")
            fig_district_trans_count = px.bar(
                district_trans_count,
                x='Districts',
                y='Transaction_count',
                title=f'Transaction Count by District in {selected_state_state_trans}',
                labels={'Transaction_count': 'Count'},
                template='plotly_white'
            )
            st.plotly_chart(fig_district_trans_count, use_container_width=True)

        else:
            st.info("No transaction data available for the selected state, year, and quarter.")
    else:
        st.warning("Mapped transaction data (map_transaction) not loaded or 'Years' column missing. Please check database connection and table schema.")

# 8. Explore by State (Users)
elif st.session_state.selected_scenario == "8. Explore by State (Users)":
    st.title(" Explore Users by State")
    st.markdown("Analyze registered users and app opens at the district level for a selected state.")

    if not df_map_user.empty and 'Years' in df_map_user.columns:
        years = sorted(df_map_user['Years'].unique())
        quarters = sorted(df_map_user['Quarter'].unique())
        states = sorted(df_map_user['States'].unique())

        col1, col2, col3 = st.columns(3)
        with col1:
            selected_year_state_user = st.selectbox("Select Year", years, key="state_user_year")
        with col2:
            selected_quarter_state_user = st.selectbox("Select Quarter", quarters, key="state_user_quarter")
        with col3:
            selected_state_state_user = st.selectbox("Select State", states, key="state_user_state")

        filtered_df_state_user = df_map_user[
            (df_map_user['Years'] == selected_year_state_user) &
            (df_map_user['Quarter'] == selected_quarter_state_user) &
            (df_map_user['States'] == selected_state_state_user)
        ].sort_values(by='RegisteredUsers', ascending=False)

        if not filtered_df_state_user.empty:
            st.subheader(f"User Data for {selected_state_state_user} ({selected_year_state_user} Q{selected_quarter_state_user})")
            st.dataframe(filtered_df_state_user)

            fig_state_user_dist = px.bar(
                filtered_df_state_user,
                x='Districts',
                y='RegisteredUsers',
                title=f'Registered Users by District in {selected_state_state_user}',
                labels={'RegisteredUsers': 'Number of Users'},
                template='plotly_white'
            )
            st.plotly_chart(fig_state_user_dist, use_container_width=True)

            fig_state_app_opens_dist = px.bar(
                filtered_df_state_user,
                x='Districts',
                y='AppOpens',
                title=f'App Opens by District in {selected_state_state_user}',
                labels={'AppOpens': 'Number of App Opens'},
                template='plotly_white'
            )
            st.plotly_chart(fig_state_app_opens_dist, use_container_width=True)

            
            st.subheader(f" Top 5 Districts by New Registered Users (QoQ) in {selected_state_state_user}")
            
            # Prepare data for QoQ calculation
            district_quarterly_users = df_map_user[
                df_map_user['States'] == selected_state_state_user
            ].groupby(['Districts', 'Years', 'Quarter'])['RegisteredUsers'].sum().reset_index()

            # Sort for correct LAG calculation
            district_quarterly_users.sort_values(by=['Districts', 'Years', 'Quarter'], inplace=True)

            # Calculate Previous Quarter Users and New Users QoQ
            district_quarterly_users['Previous_Quarter_Users'] = district_quarterly_users.groupby('Districts')['RegisteredUsers'].shift(1).fillna(0)
            district_quarterly_users['New_Users_QoQ'] = district_quarterly_users['RegisteredUsers'] - district_quarterly_users['Previous_Quarter_Users']

            # Filter for the selected year and quarter
            current_quarter_new_users = district_quarterly_users[
                (district_quarterly_users['Years'] == selected_year_state_user) &
                (district_quarterly_users['Quarter'] == selected_quarter_state_user)
            ]
            
            # Sort by new users and display top 5
            top_5_new_users = current_quarter_new_users.sort_values(by='New_Users_QoQ', ascending=False).head(5)

            st.dataframe(top_5_new_users.style.format({
                'RegisteredUsers': "{:,.0f}",
                'Previous_Quarter_Users': "{:,.0f}",
                'New_Users_QoQ': "{:,.0f}"
            }))

            # --- Visualization for Top 5 Districts by Newly Registered Users (QoQ) ---
            st.write("#### Top 5 Districts by New Registered Users (QoQ)")
            fig_top_5_new_users = px.bar(
                top_5_new_users,
                x='Districts',
                y='New_Users_QoQ',
                color='New_Users_QoQ', # Color by new users
                color_continuous_scale='Viridis', # Green-Yellow-Blue for new users
                title=f'Top 5 Districts by New Registered Users (QoQ) in {selected_state_state_user}',
                labels={'New_Users_QoQ': 'New Registered Users'},
                template='plotly_white'
            )
            st.plotly_chart(fig_top_5_new_users, use_container_width=True)


            
            st.subheader(f" App Opens by District in {selected_state_state_user}")
            district_app_opens = filtered_df_state_user.groupby('Districts')['AppOpens'].sum().reset_index().sort_values(by='AppOpens', ascending=False)
            
            st.dataframe(district_app_opens.style.format({
                'AppOpens': "{:,.0f}"
            }))

            # --- Visualization for App Opens by District ---
            st.write("#### App Opens by District")
            fig_district_app_opens = px.bar(
                district_app_opens,
                x='Districts',
                y='AppOpens',
                title=f'App Opens by District in {selected_state_state_user}',
                labels={'AppOpens': 'Number of App Opens'},
                template='plotly_white'
            )
            st.plotly_chart(fig_district_app_opens, use_container_width=True)

        else:
            st.info("No user data available for the selected state, year, and quarter.")
    else:
        st.warning("Mapped user data (map_user) not loaded or 'Years' column missing. Please check database connection and table schema.")

