# phonepe-project-1



This project is a Streamlit web application designed to analyze and visualize PhonePe Pulse data. Below is a GitHub-style explanation covering its purpose, features, technologies, and setup instructions.

PhonePe Pulse Data Analysis Dashboard
This project provides an interactive dashboard to visualize and analyze PhonePe Pulse data across various categories like transactions, user demographics, and insurance. It leverages data extracted from the PhonePe Pulse GitHub repository and stores it in a PostgreSQL database for efficient querying and visualization.

Features
Interactive Dashboards: Explore data through dynamic charts and maps.

Transaction Dynamics: Analyze transaction counts, amounts, and types across different states and quarters.

User Demographics: Gain insights into user distribution by mobile brands, registered users, and app opens at district levels.

Insurance Insights: Understand insurance transaction patterns and average premium values.

Top Metrics: Identify top pincodes for transactions, users, and insurance.

State-wise Exploration: Deep-dive into specific states for detailed transaction, user, and insurance data by district.

Year-over-Year (YoY) and Quarter-over-Quarter (QoQ) Analysis: Business case studies to compare performance over time.

Technologies Used
Python: The core programming language.

Streamlit: For building the interactive web application and dashboard.

Pandas: For data manipulation and analysis.

Plotly Express & Plotly Graph Objects: For creating interactive and visually appealing charts (bar charts, pie charts, choropleth maps, line plots).

Psycopg2: PostgreSQL database adapter for Python, used to connect and query the database.

PostgreSQL: Relational database to store the PhonePe Pulse data.

Requests: To fetch GeoJSON data for India's states for map visualizations.

Setup and Installation
To run this project locally, follow these steps:

Clone the repository:

Bash

git clone <your-repository-url>
cd <your-repository-name>
Set up a PostgreSQL Database:

Ensure you have PostgreSQL installed and running.

Create a new database (e.g., phonepe_data).

You will need to ingest the PhonePe Pulse data into this database. This project assumes you have already extracted the data from the PhonePe Pulse GitHub repository and loaded it into tables named aggregated_transaction, map_transaction, top_transaction, aggregated_user, map_user, top_user, aggregated_insurance, map_insurance, top_insurance with appropriate schemas.

Install Python Dependencies:

Bash

pip install -r requirements.txt
(You'll need to create a requirements.txt file if you don't have one. It should contain: streamlit, pandas, plotly, psycopg2-binary, sqlalchemy, requests)

Configure Database Credentials:

Open the app.py (or your main Streamlit file) and locate the DB_CONFIG dictionary.

Update the user, password, host, port, and database fields with your PostgreSQL credentials.

Security Note: For production deployments, consider using Streamlit's secrets management (.streamlit/secrets.toml) instead of hardcoding credentials directly in the script.

Run the Streamlit Application:

Bash

streamlit run app.py
This will open the dashboard in your default web browser.

Usage
Use the sidebar on the left to navigate between different analysis sections (e.g., Transaction Dynamics, User Demographics, Insurance Insights).

Within each section, use the dropdown menus and select boxes to filter data by year, quarter, state, etc., and view the corresponding visualizations.

Hover over the charts for detailed information.

