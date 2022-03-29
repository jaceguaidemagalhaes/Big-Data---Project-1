package analysethat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.io.AnsiColor._
import scala.io.StdIn.readLine
import scala.sys.process._
object AnalyseThat extends App {
  var user = "" //LOGGED USER
  var admin = false //IS THE USER AN ADMIN?
  var auth = false //SUCCESFULL AUTHENTICATION

  val userLogin = new User()
  print("\u001bc")
  banner()
  while(!auth){

    println()
    println("UserName:")
    print("> ")
    val luser = readLine().trim
    println("Password:")
    print("> ")
    val lpassword = readLine().trim

    auth = userLogin.login(luser,lpassword)
    if(!auth){
      print("\u001b[2J")
      banner()
      println(s"${RESET}${BOLD}${RED} Invalid Username/Password ${RESET}")
    } else{
      print("\u001b[2J")
      banner()
      println("Welcome "+ user.toUpperCase() + " - Admin = " + admin)
    }
    //end while
  }

  // create a spark session
  // for Windows
  //System.setProperty("hadoop.home.dir", "C:\\winutils")
  System.setProperty("hadoop.home.dir", "/usr/local/Cellar/hadoop/3.3.2/libexec")

  println("Creating Spark session....")
  Logger.getLogger("org").setLevel(Level.ERROR)//remove messages
  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir","hdfs://localhost:9000/user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  //  println("created spark session")
  //val sc = spark.sparkContext
  import spark.implicits._
  //val sc = new SparkContext()
  //spark.sql("use analysethat")
  //spark.read.json("hdfs://localhost:9000/user/analysethatJSON/homeaway/teams/").show()

  mainScreen()

  def mainScreen(): Unit ={
    var mainScreenOption = ""
    while(mainScreenOption != "9"){
      print("\u001b[2J")
      banner()
      println()
      println(s"${RESET}${BLUE}          MAIN SCREEN          ${RESET}")
      println()
      println(s"${RESET}${BLUE}Type the number of your option${RESET}")
      println("1 -> Home/away game winners for regular season.")
      println("2 -> Home/away game winners for playoffs.")
      println("3 -> Home/away game winners according to temperature range.")
      println("4 -> Home/away scores according to temperature range.")
      println("5 -> Spread hits.")
      println("6 -> Over/Under line.")
      println("7 -> Manage Users.")
      println("8 -> Manage data.")
      println("9 -> Exit system.")
      print("> ")
      mainScreenOption = readLine().trim()
      println(("Executing...."))
      mainScreenOption match {
        case "1" => {val homeAwayRegularSeason = new HomeAwayRegularSeason()
          homeAwayRegularSeason.homeAwayScreen()}
        case "2" => {val homeAwayPlayoff: HomeAwayPlayoff = new HomeAwayPlayoff()
          homeAwayPlayoff.homeAwayScreen()}
        case "3" => {val homeAwayTemperature: HomeAwayTemperature = new HomeAwayTemperature()
          homeAwayTemperature.homeAwayScreen()}
        case "4" => {val homeAwayScore: HomeAwayScore = new HomeAwayScore()
          homeAwayScore.homeAwayScreen()}
        case "5" => {val spreadHits: SpreadHits = new SpreadHits()
          spreadHits.spreadScreen()}
        case "6" => {val overUnder: OverUnder = new OverUnder()
          overUnder.overUnderScreen()}
        case "7" => userLogin.userScreen()
        case "8" => if(admin) updateDataScreen() else println("Activity restricted to Admin Users!")
        case "9" => println("Thank you!")
        case other => println("Invalid option")
      }
    //end while
    }
  //end mianScreen
  }

  def delete(): Unit = {

    try {
      println("Deleting previous JSON analyses....")
      s"hdfs dfs -rm -r /user/analysethatJSON/"!
    } catch {
      case e: Throwable => e.printStackTrace
        println("Error deleting All  JSON files.")

    }
  }

  def updateDataScreen(): Unit ={
    var selectedOption = ""
    while(selectedOption != "4"){
      print("\u001b[2J")
      banner()
      println()
      println(s"${RESET}${BLUE}          UPDATE DATA          ${RESET}")
      println()
      println(s"${RESET}${BLUE}Type the number of your option${RESET}")
      println("1 -> Download data from Kaggle.com API.")
      println("2 -> Rebuild data for analysis.")
      println("3 -> Delete previous JSON analysis.")
      println("4 -> Return to previous screen.")
      print("> ")
      selectedOption = readLine().trim()
      println(("Executing...."))
      selectedOption match {
        case "1" => {val getWebAPIData =new GetWebAPIData()
                    getWebAPIData.dowloadFilesFromWeb()
                    getWebAPIData.unzipFiles()}
        case "2" => {val prepareData = new PrepareData()
                    prepareData.create_database()}
        case "3" => delete()
        case "4" => println("Returning to previous screen")
        case other => println("Invalid option")
      }
      //end while
    }
  }


  def banner(): Unit ={
    println()
    println(s"${RESET}${BLUE_B}${BLACK}                         A       AAAATTTT   OO                        ${RESET}")
    println(s"${RESET}${BLUE_B}${BLACK}                        ATA         AT      OO                        ${RESET}")
    println(s"${RESET}${BLUE_B}${BLACK}          ANALYSE      AT TA        AT      OO    THAT!               ${RESET}")
    println(s"${RESET}${BLUE_B}${BLACK}                      ATATATA       AT                                ${RESET}")
    println(s"${RESET}${BLUE_B}${BLACK}                     AT     TA      AT      OO                        ${RESET}")
    println()
  }

//end AnalyseThat
}
