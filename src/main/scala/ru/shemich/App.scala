package ru.shemich

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructType}

object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark psql data")
      .config("spark.master", "local")
      .getOrCreate()

    runJdbcDataset(spark)
    spark.stop()

  }

  private def runJdbcDataset(spark: SparkSession): Unit = {

/*    val connectionProperties = new Properties()
    connectionProperties.put("user", "postgres")
    connectionProperties.put("password", "postgres")
    connectionProperties.put("driver", "org.postgresql.Driver")

    val regionsDF = spark.read
      .jdbc("jdbc:postgresql:spark_sber_db", "region_book", connectionProperties)

    val transactionsDF = spark.read
      .jdbc("jdbc:postgresql:spark_sber_db", "transactions", connectionProperties)*/

    import spark.implicits._
    val regionsStructureData = Seq(
      Row("Moscow",List("Lukoil","Rosneft","Sberbank", "Russian Railways")),
      Row("St Petersburg",List("Gazprom")),
      Row("Chelyabinsk",List("Red&White","Chelyabinsk Pipe")),
      Row("Tolyatti",List("AvtoVAZ")),
      Row("Vladivostok",List("DNS Group")),
      Row("Surgut",List("Surgutneftegas"))
    )

    val transactionsStructureData = Seq(
      Row("Lukoil", "Surgutneftegas", 12000L, 1L),
      Row("Lukoil", "Gazprom", 31450L, 2L),
      Row("Lukoil", "Rosneft", 5050L, 3L),
      Row("Rosneft", "Lukoil", 25000L, 4L),
      Row("Rosneft", "Surgutneftegas", 500L, 5L),
      Row("Sberbank", "Russian Railways", 50L, 6L),
      Row("Sberbank", "Red&White", 11000L, 7L),
      Row("Russian Railways", "AvtoVAZ", 3330L, 8L),
      Row("Gazprom", "Sberbank", 50600L, 9L),
      Row("Red&White", "Sberbank", 3500L, 10L),
      Row("Chelyabinsk Pipe", "Sberbank", 1200L, 11L),
      Row("AvtoVAZ", "Surgutneftegas", 1111L, 12L),
      Row("DNS Group", "AvtoVAZ", 7599L, 13L),
      Row("Surgutneftegas", "Gazprom", 1231L, 14L),
      Row("Surgutneftegas", "Lukoil", 254L, 15L),
      Row("AvtoVAZ", "Sberbank", 12L, 16L)
    )
    val regionsStructureSchema = new StructType()
      .add("region", StringType)
      .add("companies", ArrayType(StringType))

    val transactionsStructureSchema = new StructType()
      .add("company_from", StringType)
      .add("company_to", StringType)
      .add("amount", LongType)
      .add("transaction_id", LongType)

    val regionsDF = spark.createDataFrame(
      spark.sparkContext.parallelize(regionsStructureData), regionsStructureSchema)

    val transactionsDF = spark.createDataFrame(
      spark.sparkContext.parallelize(transactionsStructureData), transactionsStructureSchema)

    val allCompanies = regionsDF.select($"region", explode($"companies"))
      .sort("region").withColumnRenamed("col", "company")

    val countTo = transactionsDF.select("company_to").groupBy("company_to").count()
      .withColumnRenamed("company_to", "company")
    val countFrom = transactionsDF.select("company_from").groupBy("company_from").count()
      .withColumnRenamed("company_from", "company")

    val sumCount = countTo.unionByName(countFrom).groupBy("company").agg(sum("count") as "count")

    val countOfCompanies = allCompanies.join(sumCount, Seq("company"), joinType = "outer")
    val df = countOfCompanies.select("region", "company", "count")

    val amountTo = transactionsDF.select("company_to", "amount").groupBy("company_to").sum()
      .withColumnRenamed("company_to", "company")
      .withColumnRenamed("sum(amount)", "get")
    val amountFrom = transactionsDF.select("company_from", "amount").groupBy("company_from").sum()
      .withColumnRenamed("company_from", "company")
      .withColumnRenamed("sum(amount)", "lost")

    val mergedDF = df.join(amountTo, Seq("company"), joinType = "outer")
    val mergedDF2 = mergedDF.join(amountFrom, Seq("company"), joinType = "outer")

    val temp = mergedDF2
      .withColumn("balance", when(col("get").isNull, lit(0))
        .otherwise(col("get")) - when(col("lost").isNull, lit(0))
        .otherwise(col("lost")))
      .drop("get")
      .drop("lost")
      .select("region", "company", "balance", "count").sort("region", "balance")

    val output = temp.select("region", "company", "balance", "count").sort("region", "balance")
    output.show()
  }
}
