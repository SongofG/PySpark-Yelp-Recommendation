# Yelp Recommendation System 🌟

![Yelp Logo from Wikipedia](./images/Yelp_Logo.svg)

## Overview
This repository contains the implementation of a recommendation system for Yelp reviews using Apache Spark 🚀. The system processes large volumes of data to identify user preferences and suggest businesses they might like 🏢. The project utilizes Spark's RDDs and DataFrame APIs to perform complex data transformations and analysis 📊.

Here are the main goals and tasks for this project:

- **Data Processing** 🗂: Reads and processes JSON data from Yelp, filtering and transforming it into a structured format suitable for analysis.
- **Analysis** 🔍: Implements various transformations to deduplicate data, extract necessary fields, and calculate the count of reviews per user-business pair.
- **Recommendation Logic** 💡: Utilizes a collaborative filtering approach to recommend businesses based on user similarity(cosine similarity) and previous ratings.

## Technologies Used
- Python 🐍
- PySpark 🔥
- JSON 📄

## Setup and Configuration
```python
# Create a Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("YelpRecommendation").master("local[*]").getOrCreate()
```

## Code Example
```python
# Transform into a user-item matrix for easier manipulation
def create_user_business_star_rdd(iter):
    for x in iter:
        yield (user_index.value[x[0][0]], business_index.value[x[0][1]], x[1])

# Calculate user similarities
similarity_matrix = indexed_row_matrix.columnSimilarities()
```

## Future Improvements
### Scalability and Efficiency
* **Optimize Spark Configurations**: Different Spark configurations could have optimized the use of resources and improved the processing speed of large datasets.
* **Utilize DataFrames**: Although I wanted to test and see both RDDs and DataFrames in PySpark, transitioning from RDDs to DataFrames to leverage PySpark Optimizer could have been more efficient approach.

### Algorithm Enhancement
* **Model Improvement**: Could have utilized more ML/DL approach than consine similarity.

### Data Enrichment
* **Incorporate Additional Data Sources**: Could have used the review text data to enhance the recommendation! Moreover, integrating user demographics and geographical data could have been a possible good approach.