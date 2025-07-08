import json
import os

LARGE_POPULATION_NUMBER = 50000


# try:
# Get the directory containing the current script
current_dir = os.path.dirname(os.path.abspath(__file__))
# Construct the full path to french_cities.json
json_path = os.path.join(current_dir, "french_cities.json")

metropolitan_cities = []

with open(json_path, "r", encoding="utf-8") as file:
    french_cities = json.load(file)

for index, city in enumerate(french_cities, start=1):
    if int(city["population"]) >= LARGE_POPULATION_NUMBER:
        metropolitan_city = {
            "City_Index": index,
            "Latitude": city["lat"],
            "Longitude": city["lng"],
            "City": city["city"],
            "Region": city["admin_name"]
        }

        metropolitan_cities.append(metropolitan_city)

    try:
        with open(f"{current_dir}/metropolitan_cities.json", "w") as f:
            json.dump(metropolitan_cities, f, ensure_ascii=False)
    except FileNotFoundError:
        print(
            f"Error: Could not write french_cities at {current_dir}/metropolitan_cities.json"
        )
        raise
