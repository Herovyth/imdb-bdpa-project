def save_to_csv(dataframe, filename, output_dir="output"):
    """
    Save a Spark DataFrame as a single CSV file with header.
    """
    (
        dataframe
        .coalesce(1)
        .write
        .option("header", "true")
        .mode("overwrite")
        .csv(f"{output_dir}/{filename}")
    )
    print(f"Saved business question as csv to {output_dir}/{filename}")
