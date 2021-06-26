package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    val Array(x1,y1,x2,y2)= queryRectangle.split(",").map(_.toDouble)
    val Array(x,y)= pointString.split(",").map(_.toDouble)
    return x >=x1 && x<= x2 && y>= y1 && y <=y2
  }

  // YOU NEED TO CHANGE THIS PART

}
