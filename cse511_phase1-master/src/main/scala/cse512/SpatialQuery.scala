package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def ST_Within(pointString1:String, pointString2:String, distance:Double): Boolean = {
    var split1 = pointString1.split(",")
    var split2 = pointString2.split(",")
    var point1arr = split1.map(_.toDouble)
    var point2arr = split2.map(_.toDouble)
    // Use the (regular) distance formula to compute the distance
    val pointdist = scala.math.sqrt(scala.math.pow(point2arr(0) - point1arr(0), 2) + scala.math.pow(point2arr(1) - point1arr(1), 2))
    return (if (pointdist <= distance) (true) else (false))
  }
  def ST_Contains(queryRectangle:String, pointString:String):  Boolean = {
    var split1 = queryRectangle.split(",")
    var split2 = pointString.split(",")
    var lower_x = 0.0
    var lower_y = 0.0
    var higher_x = 0.0
    var higher_y = 0.0
    var x = split2(0).toDouble
    var y = split2(1).toDouble
    if(split1(0).toDouble < split1(2).toDouble) {
      lower_x = split1(0).toDouble
      higher_x = split1(2).toDouble
    }
    else {
      lower_x = split1(2).toDouble
      higher_x = split1(0).toDouble
    }
    if(split1(1).toDouble < split1(3).toDouble) {
      lower_y = split1(1).toDouble
      higher_y = split1(3).toDouble
    }
    else {
      lower_y = split1(3).toDouble
      higher_y = split1(1).toDouble
    }
    if(x <= higher_x && x >= lower_x && y <= higher_y && y >= lower_y){
      return true
    }
    else return false
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String) => (ST_Contains(queryRectangle, pointString)))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=> (ST_Contains(queryRectangle, pointString)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION

    spark.udf.register("ST_Within", (pointString1:String, pointString2:String, distance:Double)=>(ST_Within(pointString1,pointString2,distance)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=> (ST_Within(pointString1, pointString2, distance)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
