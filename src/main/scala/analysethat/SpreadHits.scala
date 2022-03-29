package analysethat
import scala.io.AnsiColor._
import scala.io.StdIn.readLine
import scala.sys.process._

class SpreadHits(){
//object SpreadHits extends App {
  var maxSpread:Float = 0
  var minSpread:Float = 0
  val table = "spread"

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
  //spreadScreen()
  //println(total(2020,2021,80,40))


  def spreadScreen(): Unit ={
    while(actionoption != "3") {
      println(s"${RESET}${BLUE}            SPREAD HITS${RESET}\n")
      println(s"${RESET}${BLUE}Type the number of your option:${RESET}")
      println("1 -> List saved stats.")
      println("2 -> Run new analyses.")
      println("3 -> Return to previous screen.")
      print("> ")
      actionoption = readLine().trim
      println("Executing....")
      actionoption match {
        case "1" => listAllStats()
        case "2" => spreadStats()
        case "3" => println("Exiting spread hits....")
        case other => println("Invalid Option")
      }
      // end while
    }
  }

  def listAllStats():Unit={
    try {
      val dfAllStats = spark.read.json("hdfs://localhost:9000/user/analysethatJSON/spreadhits/")
      dfAllStats.orderBy("TypeStat").show(dfAllStats.count.toInt, false)
    }catch{
      case e: Throwable => e.printStackTrace
        println("Error listing spreaderature JSON files.")
    }

  }

  def spreadStats():Unit={

    println("Type initial Year: (leave blanc for 1966)")
    var ioptionInit = readLine().trim
    println("Type the final year: (Leave blanc for 2021)")
    var ioptionFinal = readLine().trim
    println("Type higher spread limit: (as positive number)")
    var ioptionMinSpread = readLine().trim
    println("Type lower spread limit: (as positive number)")
    var ioptionMaxSpread = readLine().trim
    println("Executing....")

    var optionFinal = 0
    var optionInit = 0
    if(ioptionFinal == "") optionFinal=2021 else optionFinal = ioptionFinal.toInt
    if(ioptionInit == "") optionInit=1966 else optionInit = ioptionInit.toInt

    spreadLimits(optionInit,optionFinal)

    var spread:Float = 0
    spread = ioptionMinSpread.toFloat * -1
    var optionMinSpread = spread
    var optionMaxSpread = ioptionMaxSpread.toFloat * -1

    if(optionMinSpread > optionMaxSpread) {optionMinSpread = optionMaxSpread
      optionMaxSpread = spread}

    println("Calculating total stats....")
    val vtot = total(optionInit,optionFinal,optionMaxSpread,optionMinSpread)
    if(vtot > 0 ){
      println("Calculating home tats....")
      val vhome = home(optionInit,optionFinal,optionMaxSpread,optionMinSpread)
      println("Calculating away stats....")
      val vaway = away(optionInit,optionFinal,optionMaxSpread,optionMinSpread)
      println("Calculating draw stats....")
      val vmean = ((vhome+vaway)/vtot)*100
      val vtypeS = "SPREADHITS"+ optionInit.toString + optionFinal.toString + optionMaxSpread.toString + optionMinSpread.toString
      println("Saving JSON....")
      val df = sc.parallelize(Seq((vtypeS,vtot,vhome,vaway,vmean,minSpread * -1,maxSpread * -1))).toDF("TypeStat", "TotalGames", "HomeHits", "AwayHits", "Mean", "MaxSpread", "MinSpread")
      df.write.mode(saveMode = "Append").json("hdfs://localhost:9000/user/analysethatJSON/spreadhits/")

      println(f"General Statistic for spread hits for $vtot%6.0f games")
      println(f"Home hits from ${optionInit.toString} to ${optionFinal.toString} = $vhome")
      println(f"Away hits from ${optionInit.toString} to ${optionFinal.toString} = $vaway")
      println(f"Draws from ${optionInit.toString} to ${optionFinal.toString} = $vmean%3.2f%%")
    }else{
      println("There is no games for this season and spreaderature range!")
    }

    //end teams stats
  }

  def spreadLimits(initYear: Int, finalYear: Int): Unit ={
    val vspreadLimits = spark.sql("Select max(spread_favorite), min(spread_favorite)" +
      "from "+table+
      " where schedule_season between "+ initYear.toString + " and " + finalYear.toString ).toDF("Max", "Min").first()
    maxSpread = vspreadLimits(0).toString.toFloat * -1
    minSpread = vspreadLimits(1).toString.toFloat * -1
  }

  def total(initYear: Int, finalYear: Int, pmaxSpread: Float, pminSpread: Float): Float={
    val totalPeriod = spark.sql("Select count(*) " +
      "from "+table+
      " where schedule_season between "+ initYear.toString + " and " + finalYear.toString +
      " and spread_favorite between "+pminSpread+ " and " + pmaxSpread +
      " and score_home >= 0 and score_away >= 0 " +
      " and spread_favorite <= 0 and team_favorite_id != \"\" ").toDF("Total").first()
    val a = totalPeriod(0).toString.toFloat
    return a
  }

  def home(initYear: Int, finalYear: Int, pmaxSpread: Float, pminSpread: Float): Float={
    //home winnings
    val homeWiningsPeriod = spark.sql("Select count(*) " +
      "from "+table +
      " where schedule_season between "+ initYear.toString + " and " + finalYear.toString +
      " and spread_favorite between "+pminSpread+ " and " + pmaxSpread +
      " and  score_home > score_away" +
      " and home_team_id = team_favorite_id"
    ).toDF("HomeWinnings").first()
    val b = homeWiningsPeriod(0).toString.toFloat
    return b
  }

  def away(initYear: Int, finalYear: Int, pmaxSpread: Float, pminSpread: Float): Float={
    //home winnings
    val homeWiningsPeriod = spark.sql("Select count(*) " +
      "from "+table +
      " where schedule_season between "+ initYear.toString + " and " + finalYear.toString +
      " and spread_favorite between "+pminSpread+ " and " + pmaxSpread +
      " and  score_home < score_away" +
      " and away_team_id = team_favorite_id"
    ).toDF("HomeWinnings").first()
    val b = homeWiningsPeriod(0).toString.toFloat
    return b
  }

  //end homeaway spreaderature
}
