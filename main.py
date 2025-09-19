from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc
import matplotlib.pyplot as plt
from pathlib import Path
import pandas as pd


class BooksPipeline:
    """A data pipeline for processing book recommendation data using PySpark, based on task from Datascentics."""

    def __init__(self, app_name="BooksPipeline"):
        """
        Initialize the Spark session and set file paths to read the data.

        Parameters
        ----------
        app_name : str
            The name of the Spark application. Defaults to "BooksPipeline".
        """
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()
        self.books_csv_path = Path(__file__).parent / "bronze" / "Books.csv"
        self.ratings_csv_path = Path(__file__).parent / "bronze" / "Ratings.csv"
        self.users_csv_path = Path(__file__).parent / "bronze" / "Users.csv"

    def load_bronze(self):
        """
        Load data (bronze-level data) from CSV files into Spark DataFrames.

        Data are downloaded from https://www.kaggle.com/datasets/arashnic/book-recommendation-dataset/data
        """
        self.books = self.spark.read.csv(
            str(self.books_csv_path), header=True, inferSchema=True
        )
        self.ratings = self.spark.read.csv(
            str(self.ratings_csv_path), header=True, inferSchema=True
        )
        self.users = self.spark.read.csv(
            str(self.users_csv_path), header=True, inferSchema=True
        )

    def transform_silver(self):
        """Transform the bronze data to create silver-level data by cleaning and filtering."""
        self.books_silver = self.books.dropna(
            subset=["Book-Title", "Book-Author"]
        )  # Drop rows with null values in Book-Title or Book-Author columns
        self.ratings_silver = self.ratings.filter(
            (col("Book-Rating") >= 0) & (col("Book-Rating") <= 10)
        )  # Keep only valid ratings between 0 and 10
        self.users_silver = self.users.dropna()  # Drop rows with any null values

    def aggregate_gold(self):
        """Aggregate silver data to create gold-level data."""
        self.gold_books = (
            self.ratings_silver.groupBy("ISBN")
            .agg(
                count("Book-Rating").alias("rating_count"),
                avg("Book-Rating").alias("avg_rating"),
            )
            .join(self.books_silver, on="ISBN", how="inner")
        )

    def get_top_books(self, number_of_books=10):
        """
        Get the top most popular books based on the number of ratings.

        Parameters
        ----------
        number_of_books : int
            The number of top books to retrieve. Defaults to 10.
        """
        return (
            self.gold_books.orderBy(desc("rating_count"))
            .limit(number_of_books)
            .toPandas()
        )

    def get_top_authors(self, number_of_authors=10):
        """
        Get the top most popular authors based on the number of ratings.

        Parameters
        ----------
        number_of_authors : int
            The number of top authors to retrieve. Defaults to 10.
        """
        return (
            self.gold_books.groupBy("Book-Author")
            .agg(count("rating_count").alias("rating_count"))
            .orderBy(desc("rating_count"))
            .limit(number_of_authors)
        ).toPandas()

    def get_top_locations_of_users_who_rated_books(self, number_of_locations=10):
        """
        Get the top locations of users who rated the books.

        Parameters
        ----------
        number_of_locations : int
            The number of top locations to retrieve. Defaults to 10.
        """
        return (
            self.users_silver.join(self.ratings_silver, on="User-ID", how="inner")
            .groupBy("Location")
            .agg(count("User-ID").alias("user_count"))
            .orderBy(desc("user_count"))
            .limit(number_of_locations)
        ).toPandas()

    def get_top_age_of_users_who_rated_books(self, number_of_ages=10):
        """
        Get the top ages of users who rated the books.

        Parameters
        ----------
        number_of_ages : int
            The number of top ages to retrieve. Defaults to 10.
        """
        return (
            self.users_silver.join(self.ratings_silver, on="User-ID", how="inner")
            .groupBy("Age")
            .agg(count("User-ID").alias("user_count"))
            .orderBy(desc("user_count"))
            .limit(number_of_ages)
        ).toPandas()

    def show_top_books_graph(self, top_books: pd.DataFrame):
        """
        Show a horizontal bar graph of the top most popular books.

        Parameters
        ----------
        top_books : pd.DataFrame
            A DataFrame containing the top books with their rating counts.
        """
        plt.figure(figsize=(10, 6))
        plt.barh(top_books["Book-Title"], top_books["rating_count"])
        plt.xlabel("Number of Ratings")
        plt.ylabel("Book Title")
        plt.title("Top {} Most Popular Books".format(len(top_books)))
        plt.gca().invert_yaxis()
        plt.show(block=True)

    def show_top_authors_graph(self, top_authors: pd.DataFrame):
        """
        Show a horizontal bar graph of the top most popular authors.

        Parameters
        ----------
        top_authors : pd.DataFrame
            A DataFrame containing the top authors with their rating counts.
        """
        plt.figure(figsize=(10, 6))
        plt.barh(top_authors["Book-Author"], top_authors["rating_count"])
        plt.xlabel("Number of Ratings")
        plt.ylabel("Author")
        plt.title("Top {} Most Popular Authors".format(len(top_authors)))
        plt.gca().invert_yaxis()
        plt.show(block=True)

    def show_top_locations_of_users_graph(self, top_locations: pd.DataFrame):
        """
        Show a horizontal bar graph of the top locations of users who rated the books.

        Parameters
        ----------
        top_locations : pd.DataFrame
            A DataFrame containing the top locations with their user counts.
        """
        plt.figure(figsize=(10, 6))
        plt.barh(top_locations["Location"], top_locations["user_count"])
        plt.xlabel("Number of Users")
        plt.ylabel("Location")
        plt.title(
            "Top {} Locations of Users Who Rated Books".format(len(top_locations))
        )
        plt.gca().invert_yaxis()
        plt.show(block=True)

    def show_top_ages_of_users_graph(self, top_ages: pd.DataFrame):
        """
        Show a horizontal bar graph of the top ages of users who rated the books.

        Parameters
        ----------
        top_ages : pd.DataFrame
            A DataFrame containing the top ages with their user counts.
        """
        plt.figure(figsize=(10, 6))
        plt.barh(top_ages["Age"].astype(str), top_ages["user_count"])
        plt.xlabel("Number of Users")
        plt.ylabel("Age")
        plt.title("Top {} Ages of Users Who Rated Books".format(len(top_ages)))
        plt.gca().invert_yaxis()
        plt.show(block=True)


if __name__ == "__main__":
    pipeline = BooksPipeline()
    # ETL process:
    pipeline.load_bronze()  # Data are loaded from CSV files
    pipeline.transform_silver()  # Data are transformed, cleaned and filtered
    pipeline.aggregate_gold()

    # Visualization of data:
    # Top books
    top_books = pipeline.get_top_books(number_of_books=10)
    pipeline.show_top_books_graph(top_books)
    # Top authors
    top_authors = pipeline.get_top_authors(number_of_authors=10)
    pipeline.show_top_authors_graph(top_authors)
    # Top locations of the users who rated the books
    top_locations_of_users = pipeline.get_top_locations_of_users_who_rated_books(
        number_of_locations=10
    )
    pipeline.show_top_locations_of_users_graph(top_locations_of_users)
    # Top ages of the users who rated the books
    top_ages_of_users = pipeline.get_top_age_of_users_who_rated_books(number_of_ages=10)
    pipeline.show_top_ages_of_users_graph(top_ages_of_users)
