package analysethat
//object PrepareData {
class PrepareData(){



//    def main(args: Array[String]): Unit = {

      var path = System.getProperty("user.dir")+"/webAPIData"

      // create a spark session
      // for Windows
      //System.setProperty("hadoop.home.dir", "C:\\winutils")
//      System.setProperty("hadoop.home.dir", "/usr/local/Cellar/hadoop/3.3.2/libexec")

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

      //create_database()

// creating database
      def create_database(): Unit = {

        //drop the existing database
        println("Dropping previous database....")
        spark.sql("drop database if exists analysethat cascade")

        //create the database amd it's tables
        println("Creating new database....")
        spark.sql("create database analysethat")

        //set the analysethat as the current database
        println("Setting current Database....")
        spark.sql("use analysethat")

        // seting hive property
        spark.sql("Set hive.exec.dynamic.partition.mode=nonstrict")

        //create table teams
        println("Creating table Teams....")
        spark.sql("create table teams(name string, team_name_short string, team_id string," +
          "team_id_pfr string, team_conference_pre2002 string, team_division_pre2002 string)"+
          " partitioned by (team_conference string,team_division string)" +
          " row format delimited fields terminated by ','" +
          " stored as textfile")
        spark.sql("describe teams").show()

        //create table nfl_teams
        println("Creating table nfl_teams....")
        spark.sql("create table nfl_teams(team_name string, team_name_short string," +
          " team_id string, team_id_pfr string, team_conference string, team_division string, " +
          "team_conference_pre2002 string, team_division_pre2002 string)" +
          "  row format delimited fields terminated by ',' " +
          "  stored as textfile" +
          "  tblproperties(\"skip.header.line.count\"=\"1\")")
        spark.sql("Describe table nfl_teams").show()

        //loading data into nfl teams
        println("Loading team's data....")
        spark.sql("load data local inpath '" + path + "/nfl_teams.csv' overwrite into table nfl_teams")

        //inserting into teams
        println("Preparing team's data....")
        spark.sql("insert overwrite table teams " +
          "partition(team_conference,team_division) " +
          "select team_name, team_name_short, team_id, team_conference_pre2002, team_division_pre2002," +
          " team_id_pfr, case when team_name = 'New York Jets' then 'AFC' else team_conference end as team_conference, " +
          "case when team_name = \"Las Vegas Raiders\" then \"AFC West\" " +
          "when team_name = \"St. Louis Rams\" then 'NFC West' " +
          "when (team_division = '' and team_name != \"Las Vegas Raiders\" and team_name != \"St. Louis Rams\") then 'Old' " +
          "else team_division end as team_division from nfl_teams " +
          "sort by team_conference, team_division, team_name")

        //creating table spread_score
        println("Creating table spread_scores....")
        spark.sql("Create table spread_scores (schedule_date string, schedule_season  string, " +
          "schedule_week  string, schedule_playoff  string, team_home  string, score_home  string, " +
          "score_away  string, team_away  string, team_favorite_id  string, " +
          "spread_favorite string, over_under_line  string, stadium  string, " +
          "stadium_neutral  string, weather_temperature  string, weather_wind_mph  string, " +
          "weather_humidity  string, weather_detail  string) " +
          "Row format delimited fields terminated by ',' " +
          "stored as textfile " +
          "tblproperties(\"skip.header.line.count\"=\"1\")")
        spark.sql("describe spread_scores").show()

        //create table season data
        println("Creating table seasons_data....")
        spark.sql("Create table seasons_data (schedule_date string, schedule_season  string, " +
          "schedule_week  string,  schedule_playoff  string, team_home string, score_home  int, " +
          "score_away  int, team_away  string, team_favorite_id  string, spread_favorite decimal(5,1), " +
          "over_under_line  decimal(5,1), weather_temperature decimal(5,1)) " +
          "row format delimited fields terminated by ',' " +
          "stored as textfile")
        spark.sql("describe seasons_data").show()

        //loading spread score
        println("Loading spread_scores....")
        spark.sql("Load data local inpath '" + path + "/spreadspoke_scores.csv' overwrite into table spread_scores")

        println("Inserting seasons' data....")
        spark.sql("Insert overwrite table seasons_data " +
          "Select schedule_date, schedule_season, schedule_week, schedule_playoff, team_home, " +
          "cast(score_home as int), cast(score_away as int), team_away, team_favorite_id, " +
          "cast(spread_favorite as decimal(5,1)), cast(over_under_line as decimal(5,1)), " +
          "cast(weather_temperature as decimal(5,1)) " +
          "from spread_scores")

        //create table spread
        println("Creating spread")
        spark.sql("Create table spread(home_team_id string, score_home int, " +
          "score_away int, away_team_id string, team_favorite_id string, spread_favorite decimal(5,1)) " +
          "partitioned by (schedule_season int) " +
          "row format delimited fields terminated by ',' " +
          "stored as textfile")
        spark.sql("describe spread").show()


        //insert spread
        println("Inserting data for spread....")
        spark.sql("Insert overwrite table spread " +
          "partition(schedule_season) " +
          "Select th.team_id, sd.score_home ,sd.score_away, ta.team_id, " +
          " sd.team_favorite_id, sd.spread_favorite, cast(sd.schedule_season as int) " +
          "from seasons_data sd, teams th, teams ta " +
          "where th.name = sd.team_home " +
          "and ta.name = sd.team_away")
        //spark.sql("describe spread").show()

        //create table regular_season_home_away
        println("Creating regular_season_away_home")
        spark.sql("Create table regular_season_home_away(home_team_id string, score_home int, " +
          "score_away int, away_team_id string, schedule_week int) " +
          "partitioned by (schedule_season int) " +
          "row format delimited fields terminated by ',' " +
          "stored as textfile")
        spark.sql("describe regular_season_home_away").show()

        //insert regular_season_home_away
        println("Inserting data for regular seasons....")
        spark.sql("Insert overwrite table regular_season_home_away " +
          "partition(schedule_season) " +
          "Select th.team_id, sd.score_home ,sd.score_away, ta.team_id, cast(sd.schedule_week as int), " +
          " cast(sd.schedule_season as int) " +
          "from seasons_data sd, teams th, teams ta " +
          "where sd.schedule_playoff = 'FALSE' " +
          "and th.name = sd.team_home " +
          "and ta.name = sd.team_away")
        //spark.sql("describe regular_season_home_away").show()

        //create playoff
        println("Creating playoff_season_home_away....")
        spark.sql("Create table playoff_season_home_away(home_team_id string, score_home int, score_away int, away_team_id string, " +
          "schedule_week string) " +
          "partitioned by (schedule_season int) " +
          "row format delimited fields terminated by ',' " +
          "stored as textfile")
        spark.sql("describe playoff_season_home_away").show()

        //insert playoff data
        println("Inserting playoff's data....")
        spark.sql("Insert overwrite table playoff_season_home_away " +
          "partition(schedule_season) " +
          "Select th.team_id, sd.score_home ,sd.score_away, ta.team_id, sd.schedule_week, cast(sd.schedule_season as int) " +
          "from seasons_data sd, teams th, teams ta " +
          "where sd.schedule_playoff = 'TRUE' " +
          "and sd.schedule_week not in ('Superbowl','15') and th.name = sd.team_home " +
          "and ta.name = sd.team_away")
      //end create database
        spark.sql("show tables").show()
      }

     //spark.sql("show databases").show()

      //spark.sql("SELECT * FROM teams").show()

  //end of main
//    }

  //end of class
}
