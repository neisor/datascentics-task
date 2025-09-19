# Book Recommendation Data Pipeline

This project is a **task/homework assignment from Datascentics company** that implements a data processing pipeline for analyzing book recommendation data using PySpark and Python.

## Overview

The pipeline processes book recommendation data following a medallion architecture (Bronze → Silver → Gold) and generates visualizations to analyze the most popular books, authors, user locations, and age demographics.

## Features

- **ETL Pipeline**: Implements a complete Extract, Transform, Load process using PySpark
- **Data Quality**: Cleans and filters data to ensure quality
- **Analytics**: Aggregates data to find top books, authors, and user demographics
- **Visualizations**: Creates interactive charts showing:
  - Top most popular books (by number of ratings)
  - Top most popular authors
  - Geographic distribution of users who rated books
  - Age distribution of users who rated books

## Data Architecture

### Bronze Layer (Raw Data)
- `Books.csv`: Book information including ISBN, title, author
- `Ratings.csv`: User ratings for books
- `Users.csv`: User demographic information

*Source of the input CSV files (DataFrames): [Kaggle Book Recommendation Dataset](https://www.kaggle.com/datasets/arashnic/book-recommendation-dataset/data)*

### Silver Layer (Cleaned Data)
- Removes null values from critical fields
- Filters ratings to valid range (0-10)
- Cleanses user data

### Gold Layer (Aggregated Data)
- Combines books and ratings data
- Calculates rating counts and average ratings per book
- Ready for analytics and visualization

## Installation

1. Clone or download this repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Ensure the data files are in the `bronze/` directory

## Usage

Run the complete pipeline:

```bash
python main.py
```

The code has been **formatted and checked using Ruff**

## Main class and it's methods

### `BooksPipeline`
Main class that orchestrates the entire data processing pipeline.

#### Core Methods:
- `load_bronze()`: Load raw data from CSV files
- `transform_silver()`: Clean and filter data
- `aggregate_gold()`: Create aggregated analytics data
- `get_top_books()`: Retrieve most popular books
- `get_top_authors()`: Retrieve most popular authors
- `get_top_locations_of_users_who_rated_books()`: Get user location analytics
- `get_top_age_of_users_who_rated_books()`: Get user age analytics

#### Visualization Methods:
- `show_top_books_graph()`: Display popular books chart
- `show_top_authors_graph()`: Display popular authors chart
- `show_top_locations_of_users_graph()`: Display user location distribution
- `show_top_ages_of_users_graph()`: Display user age distribution
