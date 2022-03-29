package analysethat
import scala.io.AnsiColor._
import scala.io.StdIn.readLine

//object User extends App {
class User(){
  var query = ""
  var executeQuery:ExecuteQuery = null
  var luser = ""
  var lpassword = ""
  var ladmin = false
  var actionoption = ""
  var cryptPassword = ""
  val encrypt = new Encrypt()

  def userScreen(): Unit ={
    while(actionoption != "5") {
      println("            User Management\n")
      println(s"${RESET}${BLUE}Type the number of your option:${RESET}")
      println("1 -> List users")
      println("2 -> Updated user")
      println("3 -> New user")
      println("4 -> Delete user")
      println("5 -> Return to previous screen")
      actionoption = readLine().trim
      println("Executing....")
      actionoption match {
       case "1" => list()
       case "2" => update()
       case "3" => {if(AnalyseThat.admin) newUser() else println(s"${RESET}${RED}Task exclusive for Admins.${RESET}") }
       case "4" => {if(AnalyseThat.admin) delete() else println(s"${RESET}${RED}Task exclusive for Admins.${RESET}") }
       case "5" => println("Exiting users' management screen")
       case other => println("Wrong Option")
      }
      // end while
    }
  }

  //Delete user
  def update(): Unit={
    if(AnalyseThat.admin){
      list()
      luser = ""
      println(s"Type the username to update.")
      print("> ")
      luser = readLine().trim().toUpperCase()
      println(s"Type new password")
      print("> ")
      lpassword = readLine().trim()
      println(s"Admin (true,false)?")
      print("> ")
      ladmin = readLine().trim().toBoolean

    } else{
      println(s"Type new password")
      print("> ")
      luser =AnalyseThat.user
      lpassword = readLine().trim()
      ladmin = AnalyseThat.admin
    }



    try{
      cryptPassword = encrypt.Encrypt(lpassword)
      query = ("update user set password = \"" + cryptPassword+ "\", admin = "+ladmin+" WHERE user = \"" + luser + "\"")
      executeQuery = new ExecuteQuery(query, false)
      println(s"$luser updated")
    }catch {
      case e: Throwable => e.printStackTrace
        println(s"Error deleting $luser")
    }finally {
      if(executeQuery.connection != null)executeQuery.connection.close()
    }
    //end update user
  }


  //Delete user
  def delete(): Unit={
    list()
    luser = ""
    println(s"Type the username to delete.")
        print("> ")
        luser = readLine().trim().toUpperCase()
        try{
          query = ("DELETE FROM user WHERE user = \"" + luser + "\"")
          executeQuery = new ExecuteQuery(query, false)
          println(s"$luser deleted")
        }catch {
          case e: Throwable => e.printStackTrace
            println(s"Error deleting $luser")
        }finally {
          if(executeQuery.connection != null)executeQuery.connection.close()
        }
    //end delete user
      }


  def newUser(): Unit={

    try{
      println("Username?")
      print("> ")
      luser = readLine().trim().toUpperCase()
      println("Password?")
      print("> ")
      lpassword = readLine().trim()
      println("Admin (true,false)")
      print("> ")
      ladmin = readLine().trim().toBoolean
      cryptPassword = encrypt.Encrypt(lpassword)
      query = (s"INSERT INTO user (user, password, admin)" +
        s""" VALUES("$luser", "$cryptPassword", $ladmin)""")
      executeQuery = new ExecuteQuery(query, false)
      println(s"New user created")
    }catch {
      case e: Throwable => e.printStackTrace
        println(s"Error Creating user")
    }finally {
      if(executeQuery.connection != null)executeQuery.connection.close()
    }
    //end newUser
  }

  def list(): Unit ={
println("Users list")
    try{
      query = "SELECT * FROM user order by user"
      executeQuery = new ExecuteQuery(query, true)

      var objectMap:scala.collection.mutable.Map[String,Boolean]
      = scala.collection.mutable.Map()
      try while ( {
        executeQuery.resultSet.next
      }) {
        //load map with values
        luser = executeQuery.resultSet.getString("user").toUpperCase()
        ladmin = executeQuery.resultSet.getBoolean("admin")


        objectMap += (luser -> ladmin)

        //Display values
        print("User: " + luser)
        println(" Admin: " + ladmin)

      }


    }catch {
      case e: Throwable => e.printStackTrace
        println("Error Reading User")
    }finally {
      if (executeQuery.connection != null) executeQuery.connection.close()
    }
    //end list
  }




//  val a = login("jaceguai","123456")
//  println(a)
  def login(puser:String, ppassword:String): Boolean ={

      try{
        query = "SELECT * FROM user where user = \"" + puser.toUpperCase()+ "\""
        executeQuery = new ExecuteQuery(query, true)

        var objectMap:scala.collection.mutable.Map[String,(String,Boolean)]
        = scala.collection.mutable.Map()
        try while ( {
          executeQuery.resultSet.next
        }) {
          //load map with values
          luser = executeQuery.resultSet.getString("user").toUpperCase()
          lpassword = executeQuery.resultSet.getString("password")
          ladmin = executeQuery.resultSet.getBoolean("admin")


          objectMap += (luser -> (lpassword,ladmin))

          //Display values
//            println("User: " + luser)
//            println("Password " + lpassword)

        }
        cryptPassword = ""
        cryptPassword = encrypt.Encrypt(ppassword)
//        println("ppassword "+ ppassword)
//        println("crypt "+ cryptPassword)
//        println("object password" + objectMap(puser)._1)
//        println("object admin " + objectMap(puser)._2)
        if(cryptPassword == objectMap(puser.toUpperCase())._1){
          AnalyseThat.user = puser
          AnalyseThat.admin = objectMap(puser.toUpperCase())._2
          return true
        } else{
          return false
        }

      }catch {
        case e: Throwable => e.printStackTrace
          println("Error Reading User")
          return false
      }finally {
        if (executeQuery.connection != null) executeQuery.connection.close()
      }
    //end login
  }


}
