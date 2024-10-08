import logging
import os
import pandas as pd
import matplotlib.pyplot as plt
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import io
import base64

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Azure HTTP trigger function to analyze income disparities across different income brackets for men and women.')

    try:
        # Securely get the connection string from environment variables
        storage_account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
        storage_account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')  # Ensure the account name is set in environment variables
        
        if not storage_account_key or not storage_account_name:
            raise ValueError("Storage account credentials not found in environment variables.")
        
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"
        
        # Connect to Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client("sources")
        blob_client = container_client.get_blob_client("poverty_level_wages.csv")
        
        # Download the CSV from Blob Storage
        blob_data = blob_client.download_blob().readall()
        df = pd.read_csv(io.BytesIO(blob_data))

        # --- Calculate total for each income bracket for men and women ---
        income_brackets = {
            '0-75%': ['men_0-75%_of_poverty_wages', 'women_0-75%_of_poverty_wages'],
            '75-100%': ['men_75-100%_of_poverty_wages', 'women_75-100%_of_poverty_wages'],
            '100-125%': ['men_100-125%_of_poverty_wages', 'women_100-125%_of_poverty_wages'],
            '125-200%': ['men_125-200%_of_poverty_wages', 'women_125-200%_of_poverty_wages'],
            '200-300%': ['men_200-300%_of_poverty_wages', 'women_200-300%_of_poverty_wages'],
            '300%+': ['men_300%+_of_poverty_wages', 'women_300%+_of_poverty_wages']
        }

        bracket_totals = {bracket: df[columns].sum().values for bracket, columns in income_brackets.items()}
        bracket_df = pd.DataFrame.from_dict(bracket_totals, orient='index', columns=['Men', 'Women'])

        # --- Grouped Bar Chart ---
        plt.figure(figsize=(10, 6))
        bracket_df.plot(kind='bar')
        plt.title('Income Disparities Across Different Income Brackets for Men and Women')
        plt.xlabel('Income Bracket (% of Poverty Level)')
        plt.ylabel('Total Number of Workers')
        plt.xticks(rotation=45)
        plt.legend(title='Gender')
        plt.tight_layout()

        # Save the bar chart to an in-memory bytes buffer
        bar_chart_buffer = io.BytesIO()
        plt.savefig(bar_chart_buffer, format='png')
        bar_chart_buffer.seek(0)
        bar_chart_base64 = base64.b64encode(bar_chart_buffer.getvalue()).decode('utf-8')

        # --- Calculate Percentage Distribution ---
        bracket_df_percentage = bracket_df.div(bracket_df.sum(axis=1), axis=0) * 100

        # --- Plot Trends Over Time ---
        plt.figure(figsize=(12, 6))
        for gender in ['Men', 'Women']:
            plt.plot(df['year'], df[f'{gender.lower()}_0-75%_of_poverty_wages'], label=f'{gender} 0-75%', marker='o')
            plt.plot(df['year'], df[f'{gender.lower()}_75-100%_of_poverty_wages'], label=f'{gender} 75-100%', marker='o')
            # You can repeat this for other income brackets as necessary.

        plt.title('Trends in Income Disparities Across Different Income Brackets Over Time')
        plt.xlabel('Year')
        plt.ylabel('Number of Workers')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()

        # Save the trends chart to an in-memory bytes buffer
        trends_chart_buffer = io.BytesIO()
        plt.savefig(trends_chart_buffer, format='png')
        trends_chart_buffer.seek(0)
        trends_chart_base64 = base64.b64encode(trends_chart_buffer.getvalue()).decode('utf-8')

        # HTML response with Base64-encoded images
        html_response = f"""
        <html>
        <body>
            <h1>Income Disparities Analysis Across Different Income Brackets</h1>
            <h2>Bracket Totals (Men vs Women):</h2>
            <table border="1">
                <tr>
                    <th>Income Bracket</th>
                    <th>Men</th>
                    <th>Women</th>
                </tr>
                {"".join([f"<tr><td>{bracket}</td><td>{values[0]}</td><td>{values[1]}</td></tr>" for bracket, values in bracket_totals.items()])}
            </table>
            <h2>Grouped Bar Chart: Income Disparities</h2>
            <img src="data:image/png;base64,{bar_chart_base64}" alt="Bar Chart">
            <h2>Trends Over Time: Income Disparities</h2>
            <img src="data:image/png;base64,{trends_chart_base64}" alt="Trends Chart">
        </body>
        </html>
        """

        return func.HttpResponse(html_response, mimetype="text/html", status_code=200)

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return func.HttpResponse(f"Error occurred: {str(e)}", status_code=500)
