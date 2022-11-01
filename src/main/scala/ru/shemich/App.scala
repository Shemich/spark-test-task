package ru.shemich

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}

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
      Row("Lukoil", "Surgutneftegas", 12000, 1),
      Row("Lukoil", "Gazprom", 31450, 2),
      Row("Lukoil", "Rosneft", 5050, 3),
      Row("Rosneft", "Lukoil", 25000, 4),
      Row("Rosneft", "Surgutneftegas", 500, 5),
      Row("Sberbank", "Russian Railways", 50, 6),
      Row("Sberbank", "Red&White", 11000, 7),
      Row("Russian Railways", "AvtoVAZ", 3330, 8),
      Row("Gazprom", "Sberbank", 50600, 9),
      Row("Red&White", "Sberbank", 3500, 10),
      Row("Chelyabinsk Pipe", "Sberbank", 1200, 11),
      Row("AvtoVAZ", "Surgutneftegas", 1111, 12),
      Row("DNS Group", "AvtoVAZ", 7599, 13),
      Row("Surgutneftegas", "Gazprom", 1231, 14),
      Row("Surgutneftegas", "Lukoil", 254, 15),
      Row("AvtoVAZ", "Sberbank", 12, 16),
    )
    val regionsStructureSchema = new StructType()
      .add("region", StringType)
      .add("companies", ArrayType(StringType))

    val transactionsStructureSchema = new StructType()
      .add("company_from", StringType)
      .add("company_to", StringType)
      .add("amount", IntegerType)
      .add("transaction_id", IntegerType)

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
