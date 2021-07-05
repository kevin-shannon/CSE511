package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    // YOU NEED TO CHANGE THIS PART
    var df = pickupInfo.groupBy("x","y","z").count().withColumnRenamed("count", "freq")
    val avg = (df.agg(sum("freq")).first().getLong(0).toDouble) / numCells
    val std = math.sqrt((df.withColumn("freqS", pow(col("freq"), 2)).agg(sum("freqS")).first().getDouble(0) / numCells) - math.pow(avg, 2))

    df.createOrReplaceTempView("df")

    spark.udf.register("is_neighbor",(x1: Int,y1:Int,z1:Int,x2:Int,y2:Int,z2:Int)=>((
      HotcellUtils.is_neighbor(x1,y1,z1,x2,y2,z2)
      )))
    var processedDf = spark.sql(
      """
        |select d1.x, d1.y, d1.z, sum(d2.freq) freq
        |  from df d1, df d2
        |  where is_neighbor(d1.x, d1.y, d1.z,d2.x, d2.y, d2.z)
        |group by d1.x, d1.y, d1.z
        |""".stripMargin)
    // processedDf.createOrReplaceTempView("df2")
    processedDf.createOrReplaceTempView("df1")

    spark.udf.register("CalculateW",(x: Int, y: Int, z: Int)=>((
      HotcellUtils.CalculateW(x, y, z, minX = minX, maxX = maxX, minY = minY, maxY = maxY, minZ = minZ, maxZ = maxZ)
      )))
    processedDf = spark.sql("select x, y, z, freq, CalculateW(x, y, z) weight from df1")
    processedDf.createOrReplaceTempView("df2")

    spark.udf.register("CalculateG",(weight: Double, freq: Int)=>((
      HotcellUtils.CalculateG(weight, freq, numCells = numCells,  avg = avg, std = std)
      )))
    processedDf = spark.sql("select x, y, z from df2 order by CalculateG(weight, freq) desc")

    return processedDf // YOU NEED TO CHANGE THIS PART
  }
}
