import logging
import pandas as pd
import matplotlib.pyplot as plt
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import os
import io
import base64

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Azure HTTP trigger function to analyze wage distribution across different poverty wage ranges.')

    try:
        # Retrieve storage account credentials
        storage_account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
        storage_account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')

        if not storage_account_key or not storage_account_name:
            raise Exception("Azure Storage account credentials are missing.")

        # Connect to Blob Storage
        blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=storage_account_key)
        container_client = blob_service_client.get_container_client("sources")
        blob_client = container_client.get_blob_client("poverty_level_wages.csv")
        
        # Download the CSV from Blob Storage
        blob_data = blob_client.download_blob().readall()
        df = pd.read_csv(io.BytesIO(blob_data))

        # --- Wage Distribution Calculation ---
        wage_distribution = df[['0-75%_of_poverty_wages', '75-100%_of_poverty_wages', 
                                '100-125%_of_poverty_wages', '125-200%_of_poverty_wages', 
                                '200-300%_of_poverty_wages', '300%+_of_poverty_wages']].sum()

        total_workers = wage_distribution.sum()

        # Normalize the distribution to percentages
        wage_distribution_percentage = (wage_distribution / total_workers) * 100

        # --- Stacked Bar Chart ---
        plt.figure(figsize=(10, 6))
        plt.bar(['0-75%', '75-100%', '100-125%', '125-200%', '200-300%', '300%+'], 
                wage_distribution_percentage, color=['red', 'orange', 'yellow', 'green', 'blue', 'purple'])

        plt.title('Distribution of Wages Across Different Poverty Wage Ranges')
        plt.xlabel('Poverty Wage Range')
        plt.ylabel('Percentage of Workers (%)')
        plt.tight_layout()

        # Save the bar chart to an in-memory bytes buffer
        bar_chart_buffer = io.BytesIO()
        plt.savefig(bar_chart_buffer, format='png')
        bar_chart_buffer.seek(0)

        # Encode the image to Base64
        bar_chart_base64 = base64.b64encode(bar_chart_buffer.getvalue()).decode('utf-8')

        # --- Pie Chart ---
        plt.figure(figsize=(8, 8))
        plt.pie(wage_distribution_percentage, labels=['0-75%', '75-100%', '100-125%', '125-200%', '200-300%', '300%+'], 
                autopct='%1.1f%%', colors=['red', 'orange', 'yellow', 'green', 'blue', 'purple'])

        plt.title('Distribution of Wages Across Different Poverty Wage Ranges')
        plt.tight_layout()

        # Save the pie chart to an in-memory bytes buffer
        pie_chart_buffer = io.BytesIO()
        plt.savefig(pie_chart_buffer, format='png')
        pie_chart_buffer.seek(0)

        # Encode the pie chart to Base64
        pie_chart_base64 = base64.b64encode(pie_chart_buffer.getvalue()).decode('utf-8')

        # Combine results into JSON
        response_data = {
            "wage_distribution": wage_distribution.to_dict(),
            "total_workers": total_workers,
            "wage_distribution_percentage": wage_distribution_percentage.to_dict()
        }

        # HTML response with Base64-encoded images
        html_response = f"""
        <html>
        <body>
            <h1>Wage Distribution Analysis Across Poverty Wage Ranges</h1>
            <h2>Wage Distribution (Sum for Each Range):</h2>
            <ul>
                <li>0-75%: {wage_distribution['0-75%_of_poverty_wages']}</li>
                <li>75-100%: {wage_distribution['75-100%_of_poverty_wages']}</li>
                <li>100-125%: {wage_distribution['100-125%_of_poverty_wages']}</li>
                <li>125-200%: {wage_distribution['125-200%_of_poverty_wages']}</li>
                <li>200-300%: {wage_distribution['200-300%_of_poverty_wages']}</li>
                <li>300%+: {wage_distribution['300%+_of_poverty_wages']}</li>
            </ul>
            <h2>Stacked Bar Chart: Wage Distribution</h2>
            <img src="data:image/png;base64,{bar_chart_base64}" alt="Bar Chart">
            <h2>Pie Chart: Wage Distribution</h2>
            <img src="data:image/png;base64,{pie_chart_base64}" alt="Pie Chart">
        </body>
        </html>
        """

        return func.HttpResponse(html_response, mimetype="text/html", status_code=200)

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return func.HttpResponse(f"Error occurred: {str(e)}", status_code=500)
