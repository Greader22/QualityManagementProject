from flask import Flask, render_template_string
import requests
import json

app = Flask(__name__)

# List of Azure Function URLs
function_urls = [
    "https://project-functions.azurewebsites.net/api/DisparitiesMvsW?code=-vBhshTBCdvRj8s4-2i7nTI0OjB4ksqbx-w7cnUCzu8uAzFu3P2khQ%3D%3D",
    "https://project-functions.azurewebsites.net/api/EarningAboveLevel?code=-vBhshTBCdvRj8s4-2i7nTI0OjB4ksqbx-w7cnUCzu8uAzFu3P2khQ%3D%3D",
    "https://project-functions.azurewebsites.net/api/EducationImpactForDG?",
    "https://project-functions.azurewebsites.net/api/HourlyWagesCompMvsW?code=-vBhshTBCdvRj8s4-2i7nTI0OjB4ksqbx-w7cnUCzu8uAzFu3P2khQ%3D%3D",
    "https://project-functions.azurewebsites.net/api/PercentageChangeOverYears?code=-vBhshTBCdvRj8s4-2i7nTI0OjB4ksqbx-w7cnUCzu8uAzFu3P2khQ%3D%3D",
    "https://project-functions.azurewebsites.net/api/RaceBasedEarning?code=-vBhshTBCdvRj8s4-2i7nTI0OjB4ksqbx-w7cnUCzu8uAzFu3P2khQ%3D%3D",
    "https://project-functions.azurewebsites.net/api/TrendingWagesOverYears?code=-vBhshTBCdvRj8s4-2i7nTI0OjB4ksqbx-w7cnUCzu8uAzFu3P2khQ%3D%3D",
    "https://project-functions.azurewebsites.net/api/WageGapAndTrendOverYears?",
    "https://project-functions.azurewebsites.net/api/WageInequality?code=-vBhshTBCdvRj8s4-2i7nTI0OjB4ksqbx-w7cnUCzu8uAzFu3P2khQ%3D%3D",
    "https://project-functions.azurewebsites.net/api/WageRangesDistribution?code=-vBhshTBCdvRj8s4-2i7nTI0OjB4ksqbx-w7cnUCzu8uAzFu3P2khQ%3D%3D"
]

# Function to fetch data from a URL
def fetch_data(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()  # Assuming the response is JSON
        else:
            return f"Error: {response.status_code} for {url}"
    except Exception as e:
        return f"Error fetching data: {e}"

@app.route('/')
def display_function_outputs():
    all_outputs = {}
    
    # Loop through each function URL, fetch data, and store it
    for url in function_urls:
        output = fetch_data(url)
        all_outputs[url] = output

    # Render the HTML directly inside Python with render_template_string
    html_template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Function Outputs from Blob Storage</title>
        <style>
            body {
                font-family: Arial, sans-serif;
            }
            h1 {
                color: #333;
            }
            .function-output {
                margin-bottom: 20px;
                padding: 10px;
                border: 1px solid #ccc;
            }
            pre {
                background-color: #f9f9f9;
                padding: 10px;
                border-radius: 5px;
                overflow-x: auto;
            }
            a {
                text-decoration: none;
                color: #007BFF; /* Bootstrap primary color */
            }
            a:hover {
                text-decoration: underline;
            }
        </style>
    </head>
    <body>
        <h1>Function Outputs from Blob Storage</h1>
        
        <!-- Loop through all function outputs and display them -->
        {% for url, data in all_outputs.items() %}
            <div class="function-output">
                <h2><a href="{{ url }}" target="_blank">{{ url }}</a></h2>
                <pre>{{ data | tojson(indent=2) }}</pre> <!-- Render JSON or text -->
            </div>
        {% endfor %}
        
    </body>
    </html>
    """

    return render_template_string(html_template, all_outputs=all_outputs)

if __name__ == '__main__':
    app.run(debug=True)
