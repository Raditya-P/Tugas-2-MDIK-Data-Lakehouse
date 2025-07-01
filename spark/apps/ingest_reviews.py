import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, split, col, when

def main():
    """
    Main function to run the Spark ingestion job.
    Relies entirely on the default catalog configuration from the container.
    """
    spark = SparkSession.builder \
        .appName("IMDB Reviews Ingestion") \
        .getOrCreate()

    # The target Iceberg table. The default catalog is named 'iceberg'.
    # We will write to a table named 'reviews' in a new schema named 'imdb'.
    iceberg_table = "iceberg.imdb.reviews" 
    
    # Create the schema in the catalog if it doesn't exist
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {iceberg_table.split('.')[0]}.{iceberg_table.split('.')[1]}")

    base_path = "s3a://movieunstructured/"
    
    paths_to_process = [
        os.path.join(base_path, "train", "pos"),
        os.path.join(base_path, "train", "neg"),
        os.path.join(base_path, "test", "pos"),
        os.path.join(base_path, "test", "neg")
    ]
    
    print(f"--- Processing data from paths: {paths_to_process} ---")
    
    raw_files_df = spark.read.text(paths_to_process, wholetext=True)
    files_with_path_df = raw_files_df.withColumn("path", input_file_name())

    transformed_df = files_with_path_df.withColumn("filename", split(col("path"), "/").getItem(5)) \
        .withColumn("review_id", split(col("filename"), "_").getItem(0)) \
        .withColumn("rating_str", split(split(col("filename"), "_").getItem(1), "\\.").getItem(0)) \
        .withColumn("rating", col("rating_str").cast("int")) \
        .withColumn("sentiment", 
            when(col("path").contains("/pos/"), "positive")
            .otherwise("negative")) \
        .withColumn("dataset", 
            when(col("path").contains("/train/"), "train")
            .otherwise("test")) \
        .select(
            "review_id",
            "rating",
            "sentiment",
            "dataset",
            col("value").alias("review_text")
        )
    
    print(f"--- Writing {transformed_df.count()} records to {iceberg_table} ---")
    transformed_df.writeTo(iceberg_table).createOrReplace()

    print("--- Job finished successfully! ---")
    spark.stop()

if __name__ == '__main__':
    main()
