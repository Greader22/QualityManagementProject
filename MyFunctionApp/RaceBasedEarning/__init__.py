import logging
import os  # Ensure os is imported for environment variables
import pandas as pd
import matplotlib.pyplot as plt
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import io
import base64

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Azure HTTP trigger function to analyze share of workers earning below poverty-level wages by race.')

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

        # --- Calculations for racial groups ---
        white_mean = df['white_share_below_poverty_wages'].mean()
        black_mean = df['black_share_below_poverty_wages'].mean()
        hispanic_mean = df['hispanic_share_below_poverty_wages'].mean()

        # --- Bar Chart: Mean Share of Workers Earning Below Poverty-Level Wages by Race ---
        races = ['White', 'Black', 'Hispanic']
        mean_shares = [white_mean, black_mean, hispanic_mean]

        plt.figure(figsize=(10, 6))
        plt.bar(races, mean_shares, color=['blue', 'green', 'orange'])
        plt.title('Mean Share of Workers Earning Below Poverty-Level Wages by Race')
        plt.xlabel('Race')
        plt.ylabel('Mean Share Below Poverty-Level Wages (%)')
        plt.grid(True)
        plt.tight_layout()

        # Save the bar chart to an in-memory bytes buffer
        bar_chart_buffer = io.BytesIO()
        plt.savefig(bar_chart_buffer, format='png')
        bar_chart_buffer.seek(0)

        # Encode the image to Base64
        bar_chart_base64 = base64.b64encode(bar_chart_buffer.getvalue()).decode('utf-8')

        # --- Line Chart: Trends in Share of Workers Earning Below Poverty-Level Wages by Race Over Time ---
        plt.figure(figsize=(12, 6))
        plt.plot(df['year'], df['white_share_below_poverty_wages'], label='White', marker='o', color='blue')
        plt.plot(df['year'], df['black_share_below_poverty_wages'], label='Black', marker='o', color='green')
        plt.plot(df['year'], df['hispanic_share_below_poverty_wages'], label='Hispanic', marker='o', color='orange')

        plt.title('Trends in Share of Workers Earning Below Poverty-Level Wages by Race Over Time')
        plt.xlabel('Year')
        plt.ylabel('Share Below Poverty-Level Wages (%)')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()

        # Save the trend line chart to an in-memory bytes buffer
        trend_chart_buffer = io.BytesIO()
        plt.savefig(trend_chart_buffer, format='png')
        trend_chart_buffer.seek(0)

        # Encode the image to Base64
        trend_chart_base64 = base64.b64encode(trend_chart_buffer.getvalue()).decode('utf-8')

        # Combine results into JSON
        response_data = {
            "white_mean": white_mean,
            "black_mean": black_mean,
            "hispanic_mean": hispanic_mean
        }

        # HTML response with Base64-encoded images
        html_response = f"""
        <html>
        <body>
            <h1>Analysis of Workers Earning Below Poverty-Level Wages by Race</h1>
            <h2>Mean Share of Workers Earning Below Poverty-Level Wages by Race:</h2>
            <ul>
                <li>White: {white_mean:.2f}%</li>
                <li>Black: {black_mean:.2f}%</li>
                <li>Hispanic: {hispanic_mean:.2f}%</li>
            </ul>
            <h2>Bar Chart: Mean Share of Workers Below Poverty-Level Wages by Race</h2>
            <img src="data:image/png;base64,{bar_chart_base64}" alt="Bar Chart">
            <h2>Line Chart: Trends in Share of Workers Earning Below Poverty-Level Wages by Race Over Time</h2>
            <img src="data:image/png;base64,{trend_chart_base64}" alt="Trend Line Chart">
        </body>
        </html>
        """

        return func.HttpResponse(html_response, mimetype="text/html", status_code=200)

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return func.HttpResponse(f"Error occurred: {str(e)}", status_code=500)
