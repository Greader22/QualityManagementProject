import logging
import os  # Ensure os is imported to use os.getenv
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from sklearn.linear_model import LinearRegression
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import io
import base64

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Azure HTTP trigger function to process and visualize poverty-level wages.')

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
        download_stream = blob_client.download_blob()
        df = pd.read_csv(io.StringIO(download_stream.content_as_text()))

        # Ensure necessary columns are present
        required_columns = ['year', 'annual_poverty-level_wage']
        if not all(col in df.columns for col in required_columns):
            raise ValueError("Missing required columns in dataset")

        # Sort the DataFrame by 'year' in ascending order
        df = df.sort_values(by='year', ascending=True)

        # Calculate the year-over-year percentage change
        df['percentage_change'] = df['annual_poverty-level_wage'].pct_change() * 100

        # Create HTML for the percentage change display
        percentage_change_html = df[['year', 'annual_poverty-level_wage', 'percentage_change']].to_html(index=False)

        # Plotting the trend
        plt.figure(figsize=(10, 6))
        plt.plot(df['year'], df['annual_poverty-level_wage'], marker='o', linestyle='-', color='b')
        plt.title('Trend of Annual Poverty-Level Wages Over the Years')
        plt.xlabel('Year')
        plt.ylabel('Annual Poverty-Level Wage')
        plt.grid(True)
        plt.tight_layout()
        trend_plot_path = '/tmp/trend_plot.png'
        plt.savefig(trend_plot_path)
        plt.close()

        # Calculate a moving average to smooth the data
        df['moving_average'] = df['annual_poverty-level_wage'].rolling(window=3).mean()

        # Plot the moving average
        plt.figure(figsize=(10, 6))
        plt.plot(df['year'], df['annual_poverty-level_wage'], marker='o', linestyle='-', color='b', label='Annual Wage')
        plt.plot(df['year'], df['moving_average'], color='orange', linestyle='--', label='3-Year Moving Average')
        plt.title('Trend of Annual Poverty-Level Wages Over the Years')
        plt.xlabel('Year')
        plt.ylabel('Annual Poverty-Level Wage')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        moving_avg_plot_path = '/tmp/moving_avg_plot.png'
        plt.savefig(moving_avg_plot_path)
        plt.close()

        # Prepare the data for linear regression
        X = df['year'].values.reshape(-1, 1)
        y = df['annual_poverty-level_wage'].values

        # Create and fit a linear regression model
        model = LinearRegression()
        model.fit(X, y)

        # Predict using the model
        df['trend'] = model.predict(X)

        # Plot the trend line
        plt.figure(figsize=(10, 6))
        plt.plot(df['year'], df['annual_poverty-level_wage'], marker='o', linestyle='-', color='b', label='Annual Wage')
        plt.plot(df['year'], df['trend'], color='r', linestyle='--', label='Trend Line (Linear Regression)')
        plt.title('Trend of Annual Poverty-Level Wages Over the Years')
        plt.xlabel('Year')
        plt.ylabel('Annual Poverty-Level Wage')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        trend_line_plot_path = '/tmp/trend_line_plot.png'
        plt.savefig(trend_line_plot_path)
        plt.close()

        # Generate HTML to display the plots
        plots_html = f"""
        <html>
        <body>
            <h1>Analysis of Annual Poverty-Level Wages</h1>
            <h2>Percentage Change</h2>
            {percentage_change_html}
            <h2>Trend Plot</h2>
            <img src="data:image/png;base64,{plot_to_base64(trend_plot_path)}" alt="Trend Plot"/>
            <h2>Moving Average Plot</h2>
            <img src="data:image/png;base64,{plot_to_base64(moving_avg_plot_path)}" alt="Moving Average Plot"/>
            <h2>Linear Regression Trend Line Plot</h2>
            <img src="data:image/png;base64,{plot_to_base64(trend_line_plot_path)}" alt="Trend Line Plot"/>
        </body>
        </html>
        """

        return func.HttpResponse(
            plots_html,
            mimetype="text/html",
            status_code=200
        )
    
    except Exception as e:
        logging.error(f'Error occurred: {str(e)}')
        return func.HttpResponse(
            f"An error occurred: {str(e)}",
            status_code=500
        )

def plot_to_base64(plot_path):
    with open(plot_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')
