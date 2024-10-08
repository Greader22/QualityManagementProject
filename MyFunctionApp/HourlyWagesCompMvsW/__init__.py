import logging
import os  # Ensure os is imported for environment variables
import pandas as pd
import matplotlib.pyplot as plt
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import io
import base64

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Azure HTTP trigger function to analyze poverty-level wages for men and women.')

    try:
        # Connect to Blob Storage
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

        # --- Calculations for men and women ---
        men_mean = df['men_share_below_poverty_wages'].mean()
        women_mean = df['women_share_below_poverty_wages'].mean()

        men_median = df['men_share_below_poverty_wages'].median()
        women_median = df['women_share_below_poverty_wages'].median()

        # --- Bar Chart: Comparison of Mean Hourly Poverty-Level Wages Between Men and Women ---
        plt.figure(figsize=(10, 6))
        plt.bar(['Men', 'Women'], [men_mean, women_mean], color=['blue', 'orange'])
        plt.title('Comparison of Mean Hourly Poverty-Level Wages Between Men and Women')
        plt.xlabel('Gender')
        plt.ylabel('Mean Hourly Poverty-Level Wage')
        plt.grid(True)
        plt.tight_layout()

        # Save the bar chart to an in-memory bytes buffer
        bar_chart_buffer = io.BytesIO()
        plt.savefig(bar_chart_buffer, format='png')
        bar_chart_buffer.seek(0)

        # Encode the image to Base64
        bar_chart_base64 = base64.b64encode(bar_chart_buffer.getvalue()).decode('utf-8')

        # --- Box Plot: Distribution of Hourly Poverty-Level Wages by Gender ---
        plt.figure(figsize=(10, 6))
        plt.boxplot(
            [df['men_share_below_poverty_wages'].dropna(), df['women_share_below_poverty_wages'].dropna()],
            labels=['Men', 'Women']
        )
        plt.title('Distribution of Hourly Poverty-Level Wages by Gender')
        plt.ylabel('Hourly Poverty-Level Wage')
        plt.grid(True)
        plt.tight_layout()

        # Save the box plot to an in-memory bytes buffer
        box_plot_buffer = io.BytesIO()
        plt.savefig(box_plot_buffer, format='png')
        box_plot_buffer.seek(0)

        # Encode the image to Base64
        box_plot_base64 = base64.b64encode(box_plot_buffer.getvalue()).decode('utf-8')

        # Combine results into JSON
        response_data = {
            "men_mean": men_mean,
            "women_mean": women_mean,
            "men_median": men_median,
            "women_median": women_median
        }

        # HTML response with Base64-encoded images
        html_response = f"""
        <html>
        <body>
            <h1>Poverty-Level Wage Analysis for Men and Women</h1>
            <h2>Mean Hourly Poverty-Level Wage:</h2>
            <ul>
                <li>Men: {men_mean:.2f}%</li>
                <li>Women: {women_mean:.2f}%</li>
            </ul>
            <h2>Median Hourly Poverty-Level Wage:</h2>
            <ul>
                <li>Men: {men_median:.2f}%</li>
                <li>Women: {women_median:.2f}%</li>
            </ul>
            <h2>Bar Chart: Mean Hourly Poverty-Level Wages Comparison</h2>
            <img src="data:image/png;base64,{bar_chart_base64}" alt="Bar Chart">
            <h2>Box Plot: Hourly Poverty-Level Wages Distribution by Gender</h2>
            <img src="data:image/png;base64,{box_plot_base64}" alt="Box Plot">
        </body>
        </html>
        """

        return func.HttpResponse(html_response, mimetype="text/html", status_code=200)

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return func.HttpResponse(f"Error occurred: {str(e)}", status_code=500)
