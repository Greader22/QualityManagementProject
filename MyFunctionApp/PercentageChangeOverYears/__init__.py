import logging
import os  # Ensure os is imported for environment variables
import pandas as pd
import matplotlib.pyplot as plt
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import io
import base64

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Azure HTTP trigger function for calculating percentage change in annual poverty-level wages.')

    try:
        # Get environment variables for Azure Storage account
        storage_account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
        storage_account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
        
        if not storage_account_key or not storage_account_name:
            raise ValueError("Azure Storage account credentials are missing.")
        
        # Initialize BlobServiceClient with credentials
        blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=storage_account_key)
        container_client = blob_service_client.get_container_client("sources")
        blob_client = container_client.get_blob_client("poverty_level_wages.csv")
        
        # Download the CSV from Blob Storage
        blob_data = blob_client.download_blob().readall()
        df = pd.read_csv(io.BytesIO(blob_data))

        # --- Sort the DataFrame by 'year' in ascending order ---
        df = df.sort_values(by='year').reset_index(drop=True)

        # --- Calculate the year-over-year percentage change in annual poverty-level wages ---
        df['pct_change_poverty_wage'] = df['annual_poverty-level_wage'].pct_change() * 100

        # --- Plot the year-over-year percentage change in poverty-level wages ---
        plt.figure(figsize=(10, 6))
        plt.plot(df['year'], df['pct_change_poverty_wage'], marker='o', linestyle='-', color='blue')
        plt.title('Year-over-Year Percentage Change in Annual Poverty-Level Wages')
        plt.xlabel('Year')
        plt.ylabel('Percentage Change (%)')
        plt.grid(True)
        plt.tight_layout()

        # Save the plot to an in-memory bytes buffer
        chart_buffer = io.BytesIO()
        plt.savefig(chart_buffer, format='png')
        chart_buffer.seek(0)
        chart_base64 = base64.b64encode(chart_buffer.getvalue()).decode('utf-8')

        # Generate the HTML response with table and chart
        html_response = f"""
        <html>
        <body>
            <h1>Year-over-Year Percentage Change in Annual Poverty-Level Wages</h1>
            <h2>Percentage Change Data</h2>
            <table border="1">
                <tr>
                    <th>Year</th>
                    <th>Annual Poverty-Level Wage</th>
                    <th>Percentage Change (%)</th>
                </tr>
                {"".join([f"<tr><td>{int(row['year'])}</td><td>{row['annual_poverty-level_wage']:.2f}</td><td>{row['pct_change_poverty_wage']:.2f}%</td></tr>" for _, row in df.iterrows()])}
            </table>
            <h2>Trend Chart</h2>
            <img src="data:image/png;base64,{chart_base64}" alt="Percentage Change in Poverty-Level Wages">
        </body>
        </html>
        """

        return func.HttpResponse(html_response, mimetype="text/html", status_code=200)

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return func.HttpResponse(f"Error occurred: {str(e)}", status_code=500)
