package analysethat
import scala.io.AnsiColor._
import scala.io.StdIn.readLine
import scala.sys.process._

class OverUnder(){
  //object OverUnder extends App {
  var maxOverUnder:Float = 0
  var minOverUnder:Float = 0
  val table = "seasons_data"

  //create a spark session
  //for Windows
  //System.setProperty("hadoop.home.dir", "C:\\winutils")
//
//        System.setProperty("hadoop.home.dir", "/usr/local/Cellar/hadoop/3.3.2/libexec")
//
//        println("Creating Spark session....")
//        Logger.getLogger("org").setLevel(Level.ERROR)//remove messages
//        val spark = SparkSession
//          .builder
//          .appName("hello hive")
//          .config("spark.master", "local")
//          .config("spark.sql.warehouse.dir","hdfs://localhost:9000/user/hive/warehouse")
//          .enableHiveSupport()
//          .getOrCreate()
//        spark.sparkContext.setLogLevel("ERROR")
//        println("created spark session")
  val spark = AnalyseThat.spark
  val sc = spark.sparkContext
  import spark.implicits._

  spark.sql("use analysethat")
  //    spark.sql("describe seasons_data").show()
  var actionoption = ""
  //overUnderScreen()
  //println(total(2020,2020,60,10))


  def overUnderScreen(): Unit ={
    while(actionoption != "3") {
      println(s"${RESET}${BLUE}            OVER/UNDER LINE${RESET}\n")
      println(s"${RESET}${BLUE}Type the number of your option:${RESET}")
      println("1 -> List saved stats.")
      println("2 -> Run new analyses.")
      println("3 -> Return to previous screen.")
      print("> ")
      actionoption = readLine().trim
      println("Executing....")
      actionoption match {
        case "1" => listAllStats()
        case "2" => overUnderStats()
        case "3" => println("Exiting over/under line....")
        case other => println("Invalid Option")
      }
      // end while
    }
  }

  def listAllStats():Unit={
    try {
      val dfAllStats = spark.read.json("hdfs://localhost:9000/user/analysethatJSON/overunder/")
      dfAllStats.orderBy("TypeStat").show(dfAllStats.count.toInt, false)
    }catch{
      case e: Throwable => e.printStackTrace
        println("Error listing over/under JSON files.")
    }

  }

  def overUnderStats():Unit={

    println("Type initial Year: (leave blanc for 1966)")
    var ioptionInit = readLine().trim
    println("Type the final year: (Leave blanc for 2021)")
    var ioptionFinal = readLine().trim
    println("Type higher overUnder limit to probe:")
    var ioptionMinOverUnder = readLine().trim
    println("Type lower overUnder limit to probe:")
    var ioptionMaxOverUnder = readLine().trim
    println("Executing....")

    var optionFinal = 0
    var optionInit = 0
    if(ioptionFinal == "") optionFinal=2021 else optionFinal = ioptionFinal.toInt
    if(ioptionInit == "") optionInit=1966 else optionInit = ioptionInit.toInt

    overUnderLimits(optionInit,optionFinal)

    var overUnder:Float = 0
    overUnder = ioptionMinOverUnder.toFloat
    var optionMinOverUnder = overUnder
    var optionMaxOverUnder = ioptionMaxOverUnder.toFloat

    if(optionMinOverUnder > optionMaxOverUnder) {optionMinOverUnder = optionMaxOverUnder
      optionMaxOverUnder = overUnder}

    println("Calculating total stats....")
    val vtot = total(optionInit,optionFinal,optionMaxOverUnder,optionMinOverUnder)
    if(vtot > 0 ){
      println("Calculating over tats....")
      val vover = (over(optionInit,optionFinal,optionMaxOverUnder,optionMinOverUnder)/vtot)*100
      println("Calculating under stats....")
      val vunder = (under(optionInit,optionFinal,optionMaxOverUnder,optionMinOverUnder)/vtot)*100
      println("Calculating draw stats....")
      val vtypeS = "OVERUNDER"+ optionInit.toString + optionFinal.toString + optionMaxOverUnder.toString + optionMinOverUnder.toString
      println("Saving JSON....")
      val df = sc.parallelize(Seq((vtypeS,vtot,vover,vunder,maxOverUnder,minOverUnder))).toDF("TypeStat", "TotalGames", "Over", "Under","MaxOverUnder", "MinOverUnder")
      df.write.mode(saveMode = "Append").json("hdfs://localhost:9000/user/analysethatJSON/overunder/")

      println(f"General Statistic for Over/Under line for $vtot%6.0f games")
      println(f"Over from ${optionInit.toString} to ${optionFinal.toString} = $vover%3.2f%%")
      println(f"Under from ${optionInit.toString} to ${optionFinal.toString} = $vunder%3.2f%%")
    }else{
      println("There is no games for this season and over/under range!")
      println("Over/Under max = "+ maxOverUnder + " Over/Under min = "+ minOverUnder)
    }

    //end teams stats
  }

  def overUnderLimits(initYear: Int, finalYear: Int): Unit ={
    val voverUnderLimits = spark.sql("Select max(over_under_line), min(over_under_line)" +
      "from "+table+
      " where schedule_season between "+ initYear.toString + " and " + finalYear.toString ).toDF("Max", "Min").first()
    maxOverUnder = voverUnderLimits(0).toString.toFloat
    minOverUnder = voverUnderLimits(1).toString.toFloat
  }

  def total(initYear: Int, finalYear: Int, pmaxOverUnder: Float, pminOverUnder: Float): Float={
    val totalPeriod = spark.sql("Select count(*) " +
      "from "+table+
      " where schedule_season between "+ initYear.toString + " and " + finalYear.toString +
      " and over_under_line between "+pminOverUnder+ " and " + pmaxOverUnder +
      " and score_home >= 0 and score_away >= 0 " +
      " and over_under_line >= 0").toDF("Total").first()
    val a = totalPeriod(0).toString.toFloat
    return a
  }

  def over(initYear: Int, finalYear: Int, pmaxOverUnder: Float, pminOverUnder: Float): Float={
    val homeWiningsPeriod = spark.sql("Select count(*) " +
      "from "+table +
      " where schedule_season between "+ initYear.toString + " and " + finalYear.toString +
      " and over_under_line between "+pminOverUnder+ " and " + pmaxOverUnder +
      " and  over_under_line > (score_home + score_away)"
    ).toDF("HomeWinnings").first()
    val b = homeWiningsPeriod(0).toString.toFloat
    return b
  }

  def under(initYear: Int, finalYear: Int, pmaxOverUnder: Float, pminOverUnder: Float): Float={
    val homeWiningsPeriod = spark.sql("Select count(*) " +
      "from "+table +
      " where schedule_season between "+ initYear.toString + " and " + finalYear.toString +
      " and over_under_line between "+pminOverUnder+ " and " + pmaxOverUnder +
      " and over_under_line < (score_home + score_away)"
    ).toDF("HomeWinnings").first()
    val b = homeWiningsPeriod(0).toString.toFloat
    return b
  }

  //end homeaway overUndererature
}