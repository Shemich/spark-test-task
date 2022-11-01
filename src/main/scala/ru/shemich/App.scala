package ru.shemich

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.util.Properties

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
    val regionsDF = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql:sber_spark_db")
      .option("dbtable", "region_book")
      .option("user", "postgres")
      .option("password", "t2120112!")
      .load()
    import spark.implicits._

    val connectionProperties = new Properties()
    connectionProperties.put("user", "postgres")
    connectionProperties.put("password", "t2120112!")
    connectionProperties.put("driver", "org.postgresql.Driver")

    val transactionsDF = spark.read
      .jdbc("jdbc:postgresql:sber_spark_db", "transactions", connectionProperties)

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
    //
    val mergedDF = df.join(amountTo, Seq("company"), joinType = "outer")
    val mergedDF2 = mergedDF.join(amountFrom, Seq("company"), joinType = "outer")
    //
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