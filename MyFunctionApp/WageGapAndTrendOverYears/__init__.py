import logging
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import io
import base64

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Azure HTTP trigger function processed a request.')

    # Check if we received a year parameter
    specific_year = req.params.get('year')
    education_level = req.params.get('education_level')

    if not specific_year:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            specific_year = req_body.get('year')
            education_level = req_body.get('education_level')

    if not specific_year:
        # If no year parameter is provided, return the HTML form
        html_response = """
        <html>
        <body>
            <h1>Select Year and Education Level</h1>
            <form action="" method="get">
                <label for="year">Year:</label>
                <input type="text" id="year" name="year" placeholder="e.g., 2020" required>
                <br>
                <label for="educationLevel">Education Level:</label>
                <select id="educationLevel" name="education_level">
                    <option value="less_than_hs">Less than High School</option>
                    <option value="high_school">High School</option>
                    <option value="some_college">Some College</option>
                    <option value="bachelors_degree">Bachelor's Degree</option>
                    <option value="advanced_degree">Advanced Degree</option>
                </select>
                <br>
                <button type="submit">Generate Chart</button>
            </form>
        </body>
        </html>
        """
        return func.HttpResponse(html_response, mimetype="text/html", status_code=200)

    # Validate year and education level
    if not specific_year.isdigit():
        return func.HttpResponse("Invalid year. Please provide a valid year.", status_code=400)
    
    specific_year = int(specific_year)
    education_levels = ['less_than_hs', 'high_school', 'some_college', 'bachelors_degree', 'advanced_degree']
    if education_level not in education_levels:
        return func.HttpResponse(f"Invalid education level. Choose from {education_levels}.", status_code=400)

    try:
        # Initialize the BlobServiceClient
        storage_account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
        storage_account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account_name}.blob.core.windows.net", 
            credential=storage_account_key
        )
        
        # Connect to Blob Storage
        container_client = blob_service_client.get_container_client("sources")
        blob_client = container_client.get_blob_client("wages_by_education.csv")
        
        # Download the CSV from Blob Storage
        blob_data = blob_client.download_blob().readall()
        df = pd.read_csv(io.BytesIO(blob_data))

        # Calculate total population and proportions
        df['total_population'] = df[[f'men_{level}' for level in education_levels]].sum(axis=1)
        for level in education_levels:
            df[f'prop_men_{level}'] = df[f'men_{level}'] / df['total_population']
            df[f'prop_women_{level}'] = df[f'women_{level}'] / df['total_population']

        # Filter data for the selected year
        year_data = df[df['year'] == specific_year]

        if year_data.empty:
            return func.HttpResponse(f"No data available for year {specific_year}.", status_code=404)

        # Plot line chart for education level
        plt.figure(figsize=(12, 6))
        sns.lineplot(data=df, x='year', y=f'prop_men_{education_level}', label=f'Men with {education_level.replace("_", " ").title()}')
        sns.lineplot(data=df, x='year', y=f'prop_women_{education_level}', label=f'Women with {education_level.replace("_", " ").title()}')

        plt.title(f'Trends in {education_level.replace("_", " ").title()} Attainment Over Time')
        plt.xlabel('Year')
        plt.ylabel('Proportion')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()

        # Save the line chart to an in-memory bytes buffer
        line_chart_buffer = io.BytesIO()
        plt.savefig(line_chart_buffer, format='png')
        line_chart_buffer.seek(0)
        line_chart_base64 = base64.b64encode(line_chart_buffer.getvalue()).decode('utf-8')

        # Plot bar chart for selected year
        plt.figure(figsize=(12, 6))
        for level in education_levels:
            plt.bar(f'{level} (Men)', year_data[f'prop_men_{level}'].values[0], label=f'Men {level}', alpha=0.7)
            plt.bar(f'{level} (Women)', year_data[f'prop_women_{level}'].values[0], label=f'Women {level}', alpha=0.5)

        plt.title(f'Education Level Distribution for {specific_year}')
        plt.xlabel('Education Level')
        plt.ylabel('Proportion')
        plt.xticks(rotation=90)
        plt.legend()
        plt.tight_layout()

        # Save the bar chart to an in-memory bytes buffer
        bar_chart_buffer = io.BytesIO()
        plt.savefig(bar_chart_buffer, format='png')
        bar_chart_buffer.seek(0)
        bar_chart_base64 = base64.b64encode(bar_chart_buffer.getvalue()).decode('utf-8')

        # Generate the HTML response
        html_response = f"""
        <html>
        <body>
            <h1>Charts for Education Levels</h1>
            <form action="" method="get">
                <label for="year">Year:</label>
                <input type="text" id="year" name="year" placeholder="e.g., 2020" required>
                <br>
                <label for="educationLevel">Education Level:</label>
                <select id="educationLevel" name="education_level">
                    <option value="less_than_hs">Less than High School</option>
                    <option value="high_school">High School</option>
                    <option value="some_college">Some College</option>
                    <option value="bachelors_degree">Bachelor's Degree</option>
                    <option value="advanced_degree">Advanced Degree</option>
                </select>
                <br>
                <button type="submit">Generate Charts</button>
            </form>
            <h2>Trend Chart for {education_level.replace("_", " ").title()}</h2>
            <img src="data:image/png;base64,{line_chart_base64}" alt="Trend Chart">
            <h2>Education Level Distribution for {specific_year}</h2>
            <img src="data:image/png;base64,{bar_chart_base64}" alt="Bar Chart">
        </body>
        </html>
        """

        return func.HttpResponse(html_response, mimetype="text/html", status_code=200)

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return func.HttpResponse(f"Error occurred: {str(e)}", status_code=500)
