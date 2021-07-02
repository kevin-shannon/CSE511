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
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*).groupBy("x", "y", "z").count().withColumn("square count", col("count")*col("count"))
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  import scala.math.sqrt
  import spark.sqlContext.implicits._
  
  val x_bar = pickupInfo.agg(sum("count")).first.getLong(0).toDouble / numCells
  val s = sqrt(pickupInfo.agg(sum("square count")).first.getLong(0).toDouble / numCells - x_bar*x_bar)
  var hotcells = Seq((0,0,0,0.0))
  var xij = Array.ofDim[Int]((maxX - minX + 1).toInt,(maxY - minY + 1).toInt,(maxZ - minZ + 1))

  for (row <- pickupInfo.rdd.collect) {
    var rowsplit = row.mkString(",").split(",")
    xij(rowsplit(0).toInt - minX.toInt)(rowsplit(1).toInt - minY.toInt)(rowsplit(2).toInt - minZ.toInt) = rowsplit(3).toInt
  }

  for (x <- minX.toInt to maxX.toInt) {
    for (y <- minY.toInt to maxY.toInt) {
      for (z <- minZ to maxZ) {
        var raw_score = 0.0
        var neighbors = 0.0
        for (dx <- -1 to 1) {
          for (dy <- -1 to 1) {
            for (dz <- -1 to 1) {
              if (x+dx >= minX && x+dx <= maxX && y+dy >= minY && y+dy <= maxY && z+dz >= minZ && z+dz <= maxZ) {
                raw_score += xij(x+dx - minX.toInt)(y+dy - minY.toInt)(z+dz - minZ.toInt)
                neighbors += 1
              }
            }
          }
        }
        // don't bother if the raw score is 0
        if (raw_score != 0.0) {
          var numerator = raw_score - x_bar * neighbors
          var denominator = s * sqrt((numCells * neighbors - neighbors * neighbors)/(numCells - 1))
          var g = numerator / denominator
          hotcells = hotcells :+ (x,y,z,g)
        }
      }
    }
  }
  val df = hotcells.toDF("x", "y", "z", "g")

  return df.orderBy(col("g").desc).select("x", "y", "z")
}
}
