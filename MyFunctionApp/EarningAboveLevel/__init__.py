import logging
import os
import pandas as pd
import matplotlib.pyplot as plt
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import io
import base64

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Azure HTTP trigger function for analyzing workers earning above 300% of poverty level.')

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

        # --- Calculate total number of workers for each year ---
        df['total_workers'] = df[['0-75%_of_poverty_wages', 
                                  '75-100%_of_poverty_wages', 
                                  '100-125%_of_poverty_wages', 
                                  '125-200%_of_poverty_wages', 
                                  '200-300%_of_poverty_wages', 
                                  '300%+_of_poverty_wages']].sum(axis=1)

        # --- Calculate the proportion of workers earning above 300% of poverty wages ---
        df['proportion_above_300%'] = df['300%+_of_poverty_wages'] / df['total_workers']

        # --- Plot the proportion of workers earning above 300% of the poverty level over time ---
        plt.figure(figsize=(10, 6))
        plt.plot(df['year'], df['proportion_above_300%'], marker='o', linestyle='-', color='blue')
        plt.title('Proportion of Workers Earning Above 300% of Poverty Level Over Time')
        plt.xlabel('Year')
        plt.ylabel('Proportion of Workers (300%+ of Poverty Level)')
        plt.grid(True)
        plt.tight_layout()

        # Save the plot to an in-memory bytes buffer
        line_chart_buffer = io.BytesIO()
        plt.savefig(line_chart_buffer, format='png')
        line_chart_buffer.seek(0)
        line_chart_base64 = base64.b64encode(line_chart_buffer.getvalue()).decode('utf-8')

        # Generate the HTML response
        html_response = f"""
        <html>
        <body>
            <h1>Proportion of Workers Earning Above 300% of Poverty Level Over Time</h1>
            <h2>Proportion Data</h2>
            <table border="1">
                <tr>
                    <th>Year</th>
                    <th>Proportion of Workers (300%+)</th>
                </tr>
                {"".join([f"<tr><td>{int(row['year'])}</td><td>{row['proportion_above_300%']:.2%}</td></tr>" for _, row in df.iterrows()])}
            </table>
            <h2>Trend Chart</h2>
            <img src="data:image/png;base64,{line_chart_base64}" alt="Proportion of Workers Earning Above 300% of Poverty Level">
        </body>
        </html>
        """

        return func.HttpResponse(html_response, mimetype="text/html", status_code=200)

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return func.HttpResponse(f"Error occurred: {str(e)}", status_code=500)
