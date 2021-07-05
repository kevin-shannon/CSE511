package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // YOU NEED TO CHANGE THIS PART
  def CalculateW (x: Int, y: Int, z: Int, minX: Double, maxX: Double, minY: Double, maxY: Double, minZ: Double, maxZ: Double): Double =
  {
    var result = 0
    var cnt = 0

    if (x == minX || x == maxX) {
      cnt += 1
    }
    if (y == minY || y == maxY) {
      cnt += 1
    }
    if (z == minZ || z == maxZ) {
      cnt += 1
    }

    cnt match {
      case 1 => result = 18
      case 2 => result = 12
      case 3 => result = 8
      case _ => result = 27
    }
    return result
  }

  def CalculateG (weight: Double, freq: Int, numCells: Double, avg: Double, std: Double): Double =
  {
    return (freq.toDouble - (avg * weight)) / (std * math.sqrt((( weight * numCells.toDouble) - (weight * weight)) / (numCells.toDouble - 1.0)))
  }

  def is_neighbor(x1:Int,y1:Int,z1:Int,x2:Int,y2:Int,z2:Int): Boolean={
    return scala.math.abs(x1-x2) <=1 && scala.math.abs(y1-y2) <=1 && scala.math.abs(z1-z2) <=1
  }
}
