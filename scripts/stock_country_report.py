import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType

spark = SparkSession.builder.getOrCreate()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", "-i",
                        help="S3 file pointer where data are stored",
                        required=True)
    parser.add_argument("--output", "-o",
                        help="S3 file pointer where to write data",
                        required=True)

    args = parser.parse_args()

    # To make it efficient, we already define the schema
    schema = StructType()\
         .add("InvoiceNo", StringType(), True)\
         .add("StockCode", StringType(), True)\
         .add("Description", StringType(), True)\
         .add("Quantity", IntegerType(), True)\
         .add("InvoiceDate", StringType(), True)\
         .add("UnitPrice", FloatType(), True)\
         .add("CustomerID", StringType(), True)\
         .add("Country", StringType(), True)


    # Read the data
    print("Reading the data...")
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .load(args.input)

    # Process the data
    print("Processing the data (lazy)...")
    report = df.groupBy("StockCode", "Country").sum("Quantity")

    # Write out the data
    print("Outputting the report...")
    report.write.option("header", "true").csv(args.output)
