package analysethat
import scala.io.AnsiColor._
import scala.io.StdIn.readLine
import scala.sys.process._

class HomeAwayScore(){
//  object HomeAwayScore extends App {
  var maxTemp:Float = 0
  var minTemp:Float = 0
  val table = "seasons_data"

  //create a spark session
  //for Windows
  //System.setProperty("hadoop.home.dir", "C:\\winutils")

//      System.setProperty("hadoop.home.dir", "/usr/local/Cellar/hadoop/3.3.2/libexec")
//
//      println("Creating Spark session....")
//      Logger.getLogger("org").setLevel(Level.ERROR)//remove messages
//      val spark = SparkSession
//        .builder
//        .appName("hello hive")
//        .config("spark.master", "local")
//        .config("spark.sql.warehouse.dir","hdfs://localhost:9000/user/hive/warehouse")
//        .enableHiveSupport()
//        .getOrCreate()
//      spark.sparkContext.setLogLevel("ERROR")
//      println("created spark session")
  val spark = AnalyseThat.spark
  val sc = spark.sparkContext
  import spark.implicits._

  spark.sql("use analysethat")
  //    spark.sql("describe seasons_data").show()
  var actionoption = ""
  homeAwayScreen()
  //println(total(2020,2021,80,40))


  def homeAwayScreen(): Unit ={
    while(actionoption != "3") {
      println(s"${RESET}${BLUE}            HOME/AWAY SCORES TEMPERATURE${RESET}\n")
      println(s"${RESET}${BLUE}Type the number of your option:${RESET}")
      println("1 -> List saved stats.")
      println("2 -> Run new analyses.")
      println("3 -> Return to previous screen.")
      print("> ")
      actionoption = readLine().trim
      println("Executing....")
      actionoption match {
        case "1" => listAllStats()
        case "2" => tempStats()
        case "3" => println("Exiting Home/Away Score Temperature")
        case other => println("Wrong Option")
      }
      // end while
    }
  }

  def listAllStats():Unit={
    try {
      val dfAllStats = spark.read.json("hdfs://localhost:9000/user/analysethatJSON/temperaturescore/")
      dfAllStats.orderBy("TypeStat").show(dfAllStats.count.toInt, false)
    }catch{
      case e: Throwable => e.printStackTrace
        println("Error listing temperature score JSON files.")
    }

  }

  def tempStats():Unit={

    println("Type initial Year: (leave blanc for 1966)")
    var ioptionInit = readLine().trim
    println("Type the final year: (Leave blanc for 2021)")
    var ioptionFinal = readLine().trim
    println("Type upper temperature limit:")
    var ioptionMinTemp = readLine().trim
    println("Type lower temperature limit:")
    var ioptionMaxTemp = readLine().trim
    println("Executing....")

    var optionFinal = 0
    var optionInit = 0
    if(ioptionFinal == "") optionFinal=2021 else optionFinal = ioptionFinal.toInt
    if(ioptionInit == "") optionInit=1966 else optionInit = ioptionInit.toInt

    tempLimits(optionInit,optionFinal)

    var temp:Float = 0
    temp = ioptionMinTemp.toFloat
    var optionMinTemp = temp
    var optionMaxTemp = ioptionMaxTemp.toFloat

    if(optionMinTemp.toFloat > optionMaxTemp.toFloat) {optionMinTemp = optionMaxTemp
      optionMaxTemp = temp}

    println("Calculating total stats....")
    val vtot = total(optionInit,optionFinal,optionMaxTemp,optionMinTemp)
    if(vtot > 0 ){
      println("Calculating home tats....")
      val vhome = home(optionInit,optionFinal,optionMaxTemp,optionMinTemp)
      println("Calculating away stats....")
      val vaway = away(optionInit,optionFinal,optionMaxTemp,optionMinTemp)
      val vtypeS = "TEMPSCORE"+ optionInit.toString + optionFinal.toString + optionMaxTemp.toString + optionMinTemp.toString
      println("Saving JSON....")
      val mean = (vhome+vaway)/vtot
      val df = sc.parallelize(Seq((vtypeS,vtot,vhome,vaway,mean,maxTemp,minTemp))).toDF("TypeStat", "TotalGames", "HomeScores", "AwayScores", "Mean", "MaxTemp", "MinTemp")
      df.write.mode(saveMode = "Append").json("hdfs://localhost:9000/user/analysethatJSON/temperaturescore/")

      println(f"General Statistic for home and away results from $vtot%6.0f games")
      println(f"Home scores from ${optionInit.toString} to ${optionFinal.toString} = $vhome")
      println(f"Away scores from ${optionInit.toString} to ${optionFinal.toString} = $vaway")
      println(f"Scores mean ${optionInit.toString} to ${optionFinal.toString} = $mean%3.2f")
    }else{
      println("There is no games for this season and temperature range!")
    }

    //end teams stats
  }

  def tempLimits(initYear: Int, finalYear: Int): Unit ={
    val vtempLimits = spark.sql("Select max(weather_temperature), min(weather_temperature)" +
      "from "+table+
      " where schedule_season between "+ initYear.toString + " and " + finalYear.toString ).toDF("Max", "Min").first()
    maxTemp = vtempLimits(0).toString.toFloat
    minTemp = vtempLimits(1).toString.toFloat
  }

  def total(initYear: Int, finalYear: Int, pmaxTemp: Float, pminTemp: Float): Float={
    val totalPeriod = spark.sql("Select count(*) " +
      "from "+table+
      " where schedule_season between "+ initYear.toString + " and " + finalYear.toString +
      " and weather_temperature between "+pminTemp+ " and " + pmaxTemp +
      " and score_home >= 0 and score_away >= 0").toDF("Total").first()
    val a = totalPeriod(0).toString.toFloat
    return a
  }

  def home(initYear: Int, finalYear: Int, pmaxTemp: Float, pminTemp: Float): Float={
    //home winnings
    val homeWiningsPeriod = spark.sql("Select sum(score_home) " +
      "from "+table +
      " where schedule_season between "+ initYear.toString + " and " + finalYear.toString +
      " and weather_temperature between "+pminTemp+ " and " + pmaxTemp
    ).toDF("HomeWinnings").first()
    val b = homeWiningsPeriod(0).toString.toFloat
    return b
  }

  def away(initYear: Int, finalYear: Int, pmaxTemp: Float, pminTemp: Float): Float={
    //home winnings
    val homeWiningsPeriod = spark.sql("Select sum(score_away) " +
      "from "+table +
      " where schedule_season between "+ initYear.toString + " and " + finalYear.toString +
      " and weather_temperature between "+pminTemp+ " and " + pmaxTemp
    ).toDF("HomeWinnings").first()
    val b = homeWiningsPeriod(0).toString.toFloat
    return b
  }

  //end homeaway temperature score
}