package analysethat
import scala.io.AnsiColor._
import scala.io.StdIn.readLine
import scala.sys.process._

//object HomeAwayRegularSeason extends App {
class HomeAwayRegularSeason(){

  var path = System.getProperty("user.dir")+"/webAPIData"
  val table = "regular_season_home_away"
  val typeSeason = "regular"
  //var user = "john"
  // create a spark session
  // for Windows
  //System.setProperty("hadoop.home.dir", "C:\\winutils")

//  System.setProperty("hadoop.home.dir", "/usr/local/Cellar/hadoop/3.3.2/libexec")

//  println("Creating Spark session....")
//  Logger.getLogger("org").setLevel(Level.ERROR)//remove messages
//  val spark = SparkSession
//    .builder
//    .appName("hello hive")
//    .config("spark.master", "local")
//    .config("spark.sql.warehouse.dir","hdfs://localhost:9000/user/hive/warehouse")
//    .enableHiveSupport()
//    .getOrCreate()
//  spark.sparkContext.setLogLevel("ERROR")
//  println("created spark session")
  val spark = AnalyseThat.spark
  val sc = spark.sparkContext
  import spark.implicits._
  //val sc = new SparkContext()
  spark.sql("use analysethat")

  var actionoption = ""

  def homeAwayScreen(): Unit ={
    while(actionoption != "4") {
      println(s"${RESET}${BLUE}            HOME/AWAY WINNINGS ${typeSeason.toUpperCase}${RESET}\n")
      println(s"${RESET}${BLUE}Type the number of your option:${RESET}")
      println("1 -> List saved stats for all teams.")
      println("2 -> List stats for individual teams.")
      println("3 -> Run new analyses.")
      //println("4 -> Delete previous analysis.")
      println("4 -> Return to previous screen.")
      print("> ")
      actionoption = readLine().trim
      println("Executing....")
      actionoption match {
        case "1" => listAllStats()
        case "2" => listTeamsStats()
        case "3" => teamsStats()
        //case "4" => delete()
        case "4" => println("Exiting teams home/away winnings for "+ typeSeason + " season")
        case other => println("Wrong Option")
      }
      // end while
    }
  }


  def delete(): Unit ={

      try{
      println("Deleting previous All analyses....")
      s"hdfs dfs -rm -r /user/analysethatJSON/homeaway/all/"!
    }catch {
      case e: Throwable => e.printStackTrace
        println("Error deleting All  JSON files.")

    }
      try{
        println("Deleting previous teams analyses....")
        s"hdfs dfs -rm -r /user/analysethatJSON/homeaway/teams/"!
      }catch {
        case e: Throwable => e.printStackTrace
          println("Error deleting Teams JSON files.")
      }

  }


  def listAllStats():Unit={
    try {
      val dfAllStats = spark.read.json("hdfs://localhost:9000/user/analysethatJSON/homeaway/all/")
      dfAllStats.orderBy("TypeStat").show(dfAllStats.count.toInt, false)
    }catch{
      case e: Throwable => e.printStackTrace
        println("Error listing All Teams JSON files.")
    }

  }

  def listTeamsStats():Unit={
    try{
      val dfTeamsStats = spark.read.json("hdfs://localhost:9000/user/analysethatJSON/homeaway/teams/")
      dfTeamsStats.orderBy("TypeStat").show(dfTeamsStats.count.toInt, false)
    }catch{
      case e: Throwable => e.printStackTrace
        println("Error listing Teams JSON files.")
    }

  }

  def teamsStats():Unit={
   val dfTeams = spark.sql("Select team_id, name from teams")
   dfTeams.orderBy("team_id").show(dfTeams.count.toInt, false)


   println("Type team ID or type ALL for all teams:")
   var ioptionTeam = readLine().trim.toUpperCase()
   println("Select initial Year: (leave blanc for 1966)")
   var ioptionInit = readLine().trim
   println("Select the final year: (Leave blanc for 2021)")
   var ioptionFinal = readLine().trim

   var optionFinal = 0
   var optionInit = 0
   if(ioptionFinal == "") optionFinal=2021 else optionFinal = ioptionFinal.toInt
   if(ioptionInit == "") optionInit=1966 else optionInit = ioptionInit.toInt

   if(ioptionTeam == "ALL") {

   println("Calculating total stats....")
   val vtot = total(optionInit,optionFinal)
     if(vtot > 0 ){
       println("Calculating home stats....")
       val vhome = (home(optionInit,optionFinal)/vtot)*100
       println("Calculating away stats....")
       val vaway = (away(optionInit,optionFinal)/vtot)*100
       println("Calculating draw stats....")
       val vdraw = (draw(optionInit,optionFinal)/vtot)*100
       val vtypeS = "GenHAD"+ optionInit.toString + optionFinal.toString + typeSeason
       println("Saving JSON....")
       val df = sc.parallelize(Seq((vtypeS,vtot,vhome,vaway,vdraw))).toDF("TypeStat", "Total Games", "Home", "Away", "Draw")
       df.write.mode(saveMode = "Append").json("hdfs://localhost:9000/user/analysethatJSON/homeaway/all/")

       println(f"General Statistic for home and away results from $vtot%6.0f games")
       println(f"Home winnings from ${optionInit.toString} to ${optionFinal.toString} = $vhome%2.2f%%")
       println(f"Away winnings from ${optionInit.toString} to ${optionFinal.toString} = $vaway%2.2f%%")
       println(f"Draws from ${optionInit.toString} to ${optionFinal.toString} = $vdraw%2.2f%%")
     }else{
       println("There is no games for this season range!")
     }


 } else{
   println("Calculating totals for " + ioptionTeam + "....")
   val vtothome = totalhome(ioptionTeam,optionInit,optionFinal)
   val vtotaway = totalaway(ioptionTeam,optionInit,optionFinal)
   println("Calculating home stats....")
     var vhome = 0f
     var vaway = 0f
     var vdraw = 0f
  if(vtothome > 0){
    vhome = (home(optionInit,optionFinal,ioptionTeam)/vtothome)*100
  }
    println("Calculating away stats....")
  if(vtotaway > 0){
    vaway = (away(optionInit,optionFinal,ioptionTeam)/vtotaway)*100
  }
     println("Calculating draw stats....")
     if(vtotaway > 0 & vtothome > 0){
       vdraw = (draw(optionInit,optionFinal,ioptionTeam)/(vtothome+vtotaway))*100
     }
     val vtypeS = ioptionTeam+"HAD"+ optionInit.toString + optionFinal.toString + typeSeason
     println("Saving JSON....")
     val df = sc.parallelize(Seq((vtypeS,vhome, vtothome,vaway,vtotaway, vdraw))).toDF("TypeStat", "Home", "HomeGames", "Away", "AwayGames", "Draw")
      df.write.mode(saveMode = "Append").json("hdfs://localhost:9000/user/analysethatJSON/homeaway/teams/")

   println(ioptionTeam + " statistics for home and away results")
   println(f"Home winnings from ${optionInit.toString} to ${optionFinal.toString} = $vhome%2.2f%% from $vtothome $typeSeason games")
   println(f"Away winnings from ${optionInit.toString} to ${optionFinal.toString} = $vaway%2.2f%% from $vtotaway $typeSeason games")
   println(f"Draws from ${optionInit.toString} to ${optionFinal.toString} = $vdraw%2.2f%%")
 }

   //end teams stats
 }



//spark.sql("select * from regular_season_home_away where (home_team_id = \"SF\" or away_team_id = \"SF\") and schedule_season = 2021 ").show()




  def total(initYear: Int, finalYear: Int): Float={
    val totalPeriod = spark.sql("Select count(*) " +
      "from "+table+
      " where schedule_season between "+ initYear.toString + " and " + finalYear.toString +
      " and score_home >= 0 and score_away >= 0"
    ).toDF("Total").first()
    val a = totalPeriod(0).toString.toFloat
    return a
  }

  def home(initYear: Int, finalYear: Int): Float={
    //home winnings
    val homeWiningsPeriod = spark.sql("Select count(*) " +
      "from "+table +
      " where schedule_season between "+ initYear.toString + " and " + finalYear.toString +
      " and  score_home > score_away " +
      "and score_home >= 0 and score_away >= 0"
    ).toDF("HomeWinnings").first()
    val b = homeWiningsPeriod(0).toString.toFloat
    return b
  }

  def away(initYear: Int, finalYear: Int): Float={
    //home winnings
    val homeWiningsPeriod = spark.sql("Select count(*) " +
      "from "+table +
      " where schedule_season between "+ initYear.toString + " and " + finalYear.toString +
      " and  score_home < score_away" +
      " and score_home >= 0 and score_away >= 0"
    ).toDF("HomeWinnings").first()
    val b = homeWiningsPeriod(0).toString.toFloat
    return b
  }

  def draw(initYear: Int, finalYear: Int): Float={
    //home winnings
    val homeWiningsPeriod = spark.sql("Select count(*) " +
      "from "+ table +
      " where schedule_season between "+ initYear.toString + " and " + finalYear.toString +
      " and  score_home = score_away" +
      " and score_home >= 0 and score_away >= 0"
    ).toDF("HomeWinnings").first()
    val b = homeWiningsPeriod(0).toString.toFloat
    return b
  }

  def totalhome(team:String, initYear: Int, finalYear: Int): Float={
    val totalPeriod = spark.sql("Select count(*) " +
      "from "+ table +
      " where schedule_season between "+ initYear.toString + " and " + finalYear.toString +
    " and home_team_id = \"" + team + "\"" +
      " and score_home >= 0 and score_away >= 0"
    ).toDF("Total").first()
    val a = totalPeriod(0).toString.toFloat
    return a
  }

  def totalaway(team:String, initYear: Int, finalYear: Int): Float={
    val totalPeriod = spark.sql("Select count(*) " +
      "from " + table +
      " where schedule_season between "+ initYear.toString + " and " + finalYear.toString +
      " and away_team_id = \"" + team + "\"" +
      " and score_home >= 0 and score_away >= 0"
    ).toDF("Total").first()
    val a = totalPeriod(0).toString.toFloat
    return a
  }


  def home(initYear: Int, finalYear: Int, team: String): Float={
    //home winnings
    val homeWiningsPeriod = spark.sql("Select count(*) " +
      "from "+ table +
      " where schedule_season between "+ initYear.toString + " and " + finalYear.toString +
      " and  score_home > score_away" +
    " and home_team_id = \"" + team + "\"" +
      " and score_home >= 0 and score_away >= 0"
    ).toDF("HomeWinnings").first()
    val b = homeWiningsPeriod(0).toString.toFloat
    return b
  }

  def away(initYear: Int, finalYear: Int, team: String): Float={
    //home winnings
    val homeWiningsPeriod = spark.sql("Select count(*) " +
      "from " + table +
      " where schedule_season between "+ initYear.toString + " and " + finalYear.toString +
      " and  score_home < score_away" +
      " and away_team_id = \"" + team + "\"" +
      " and score_home >= 0 and score_away >= 0"
    ).toDF("HomeWinnings").first()
    val b = homeWiningsPeriod(0).toString.toFloat
    return b
  }

  def draw(initYear: Int, finalYear: Int, team: String): Float={
    //home winnings
    val homeWiningsPeriod = spark.sql("Select count(*) " +
      "from "+ table +
      " where schedule_season between "+ initYear.toString + " and " + finalYear.toString +
      " and  score_home = score_away" +
      " and (away_team_id = \"" + team + "\" or home_team_id = \"" + team + "\")" +
      " and score_home >= 0 and score_away >= 0"
    ).toDF("HomeWinnings").first()
    val b = homeWiningsPeriod(0).toString.toFloat
    return b
  }
  //final class
}
