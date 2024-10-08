import logging
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import io
import base64

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Azure HTTP trigger function processed a request.')

    try:
        # Connect to Blob Storage
        storage_account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
        storage_account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
        container_name = "sources"

        if not storage_account_key or not storage_account_name:
            raise ValueError("Missing Azure Storage credentials.")

        # Initialize BlobServiceClient
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account_name}.blob.core.windows.net",
            credential=storage_account_key
        )
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client("wages_by_education.csv")
        
        # Download the CSV from Blob Storage
        blob_data = blob_client.download_blob().readall()
        df = pd.read_csv(io.BytesIO(blob_data))

        # Calculate total population
        df['total_population'] = df[[
            'men_less_than_hs', 'men_high_school', 'men_some_college', 'men_bachelors_degree', 'men_advanced_degree',
            'women_less_than_hs', 'women_high_school', 'women_some_college', 'women_bachelors_degree', 'women_advanced_degree',
            'white_less_than_hs', 'white_high_school', 'white_some_college', 'white_bachelors_degree', 'white_advanced_degree',
            'black_less_than_hs', 'black_high_school', 'black_some_college', 'black_bachelors_degree', 'black_advanced_degree',
            'hispanic_less_than_hs', 'hispanic_high_school', 'hispanic_some_college', 'hispanic_bachelors_degree', 'hispanic_advanced_degree'
        ]].sum(axis=1)

        # Calculate proportions
        education_levels = ['less_than_hs', 'high_school', 'some_college', 'bachelors_degree', 'advanced_degree']
        for level in education_levels:
            df[f'prop_men_{level}'] = df[f'men_{level}'] / df['total_population']
            df[f'prop_women_{level}'] = df[f'women_{level}'] / df['total_population']
            df[f'prop_white_{level}'] = df[f'white_{level}'] / df['total_population']
            df[f'prop_black_{level}'] = df[f'black_{level}'] / df['total_population']
            df[f'prop_hispanic_{level}'] = df[f'hispanic_{level}'] / df['total_population']

        # Calculate Gini coefficients
        def gini_coefficient(proportions):
            """Compute the Gini coefficient of a numpy array."""
            sorted_proportions = np.sort(proportions)
            n = len(proportions)
            cumulative_proportions = np.cumsum(sorted_proportions) / np.sum(sorted_proportions)
            cumulative_proportions = np.append([0], cumulative_proportions)
            return 1 - 2 * np.trapz(cumulative_proportions, dx=1.0 / n)

        gini_results = []
        for year in df['year'].unique():
            year_data = df[df['year'] == year]
            proportions = [year_data[f'prop_men_{level}'].values[0] for level in education_levels]
            gini_index = gini_coefficient(proportions)
            gini_results.append({'year': year, 'gini_index': gini_index})

        gini_df = pd.DataFrame(gini_results)

        # Plot Gini coefficients
        plt.figure(figsize=(12, 6))
        plt.plot(gini_df['year'], gini_df['gini_index'], marker='o')
        plt.title('Changes in Educational Attainment Inequality Over Time')
        plt.xlabel('Year')
        plt.ylabel('Gini Coefficient')
        plt.grid(True)

        # Save the Gini plot to an in-memory bytes buffer
        gini_buffer = io.BytesIO()
        plt.savefig(gini_buffer, format='png')
        gini_buffer.seek(0)
        gini_chart_base64 = base64.b64encode(gini_buffer.getvalue()).decode('utf-8')
        plt.close()

        # Plot educational attainment over time by group
        plt.figure(figsize=(14, 8))
        for level in education_levels:
            sns.lineplot(data=df, x='year', y=f'prop_men_{level}', label=f'Men with {level}')
            sns.lineplot(data=df, x='year', y=f'prop_women_{level}', label=f'Women with {level}')
            sns.lineplot(data=df, x='year', y=f'prop_white_{level}', label=f'White with {level}')
            sns.lineplot(data=df, x='year', y=f'prop_black_{level}', label=f'Black with {level}')
            sns.lineplot(data=df, x='year', y=f'prop_hispanic_{level}', label=f'Hispanic with {level}')

        plt.title('Educational Attainment Over Time by Group')
        plt.xlabel('Year')
        plt.ylabel('Proportion')
        plt.legend()

        # Save the educational attainment plot to an in-memory bytes buffer
        attainment_buffer = io.BytesIO()
        plt.savefig(attainment_buffer, format='png')
        attainment_buffer.seek(0)
        attainment_chart_base64 = base64.b64encode(attainment_buffer.getvalue()).decode('utf-8')
        plt.close()

        # Calculate ratios
        df['ratio_bachelors_to_less_than_hs'] = df['prop_men_bachelors_degree'] / df['prop_men_less_than_hs']
        df['ratio_women_bachelors_to_less_than_hs'] = df['prop_women_bachelors_degree'] / df['prop_women_less_than_hs']

        # Plot ratios
        plt.figure(figsize=(14, 8))
        sns.lineplot(data=df, x='year', y='ratio_bachelors_to_less_than_hs', label='Men: Bachelors to Less Than HS')
        sns.lineplot(data=df, x='year', y='ratio_women_bachelors_to_less_than_hs', label='Women: Bachelors to Less Than HS')
        plt.title('Ratio of Higher to Lower Education Levels Over Time')
        plt.xlabel('Year')
        plt.ylabel('Ratio')
        plt.legend()

        # Save the ratio plot to an in-memory bytes buffer
        ratio_buffer = io.BytesIO()
        plt.savefig(ratio_buffer, format='png')
        ratio_buffer.seek(0)
        ratio_chart_base64 = base64.b64encode(ratio_buffer.getvalue()).decode('utf-8')
        plt.close()

        # Generate the HTML response
        html_response = f"""
        <html>
        <body>
            <h1>Educational Attainment Analysis</h1>
            <h2>Changes in Educational Attainment Inequality Over Time</h2>
            <img src="data:image/png;base64,{gini_chart_base64}" alt="Gini Coefficient Chart">
            <h2>Educational Attainment Over Time by Group</h2>
            <img src="data:image/png;base64,{attainment_chart_base64}" alt="Educational Attainment Chart">
            <h2>Ratio of Higher to Lower Education Levels Over Time</h2>
            <img src="data:image/png;base64,{ratio_chart_base64}" alt="Ratio Chart">
        </body>
        </html>
        """

        return func.HttpResponse(html_response, mimetype="text/html", status_code=200)

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return func.HttpResponse(f"Error occurred: {str(e)}", status_code=500)
