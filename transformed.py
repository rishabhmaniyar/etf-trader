import requests
from transformers import pipeline

# Load pre-trained language model
nlp = pipeline("text-classification", model="distilbert-base-uncased-finetuned-sst-2-english", device=0)

# API URL
url = "https://news-headlines.tradingview.com/v2/view/headlines/symbol?client=web&lang=en&symbol=NSE%3AINFY"

# Fetch data from API
response = requests.get(url)
data = response.json()

# Function to filter upgrade recommendations
def filter_upgrade_recommendations(headlines):
    upgrade_keywords = ['buy', 'upgrade', 'raise', 'increase', 'boost']
    recommendations = []
    for headline in headlines:
        print("Processing ---- ", headline)
        if any(keyword in headline.lower() for keyword in upgrade_keywords):
            print("Inner process ---- ", headline)
            try:
                result = nlp(headline)
                print("RESULT ---- ", result)
                if result[0]['label'] == 'POSITIVE':
                    print("Found positive", headline)
                    recommendations.append(headline)
            except Exception as e:
                print("Error processing headline:", headline)
                print("Exception:", e)
    return recommendations

# Assuming the headlines are under the key 'items'
headlines = [item['title'] for item in data['items']]

# Filter upgrade recommendations
upgrade_recommendations = filter_upgrade_recommendations(headlines)

# Print filtered recommendations
for recommendation in upgrade_recommendations:
    print(recommendation)
