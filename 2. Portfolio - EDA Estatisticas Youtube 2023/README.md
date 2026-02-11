# Exploratory Data Analysis – Global YouTube Statistics 2023

## Project Overview

This project presents an Exploratory Data Analysis (EDA) of global YouTube channel statistics for 2023.
The objective is to understand how top YouTube channels are distributed across countries and content categories, and to analyze patterns related to subscribers, views and channel characteristics.

## Objectives

* Explore the global distribution of YouTube channels
* Analyze subscribers and views across countries and categories
* Identify patterns and outliers in channel performance
* Practice data cleaning and exploratory analysis on a real-world dataset

## Dataset

* **Source:** Kaggle – Global YouTube Statistics 2023 (https://www.kaggle.com/datasets/nelgiriyewithana/global-youtube-statistics-2023/discussion/442205)
* **Description:** The dataset contains information about popular YouTube channels worldwide, including subscribers, total views, uploads, country, content category and estimated earnings.

## Project Structure

```
youtube-eda-2023/
├── data/
│   └── raw/
│       └── Global_YouTube_Statistics.csv
├── notebooks/
│   └── 01_eda_global_youtube.ipynb
├── images/
├── README.md
└── requirements.txt
```

## Analysis Steps

1. Data loading and initial inspection
2. Data quality assessment (missing values, data types)
3. Basic data cleaning
4. Exploratory analysis using visualizations
5. Interpretation of results and insights

## Key Insights

* A small number of countries concentrate most of the top YouTube channels
* Entertainment-related categories dominate in terms of subscribers and views
* Subscriber and view distributions are highly skewed, with strong outliers
* Older channels tend to accumulate higher total views

## Tools & Technologies

* Python
* Pandas & NumPy
* Matplotlib & Seaborn
* Jupyter Notebook
* VS Code

## How to Run

1. Clone this repository
2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```
3. Open the notebook using Jupyter Notebook or VS Code
4. Run the cells in order

## Limitations

* The dataset represents only a subset of YouTube channels
* Some variables contain missing or estimated values
* The analysis focuses on descriptive insights, without predictive modeling

## Next Steps

* Deeper analysis by country and category
* Feature engineering for machine learning tasks
* Clustering channels based on performance metrics
* Time-based analysis using channel creation year

---

📫 **Author**
João Victor Assunção Pereira

