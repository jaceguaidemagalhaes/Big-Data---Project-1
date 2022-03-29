package analysethat

import java.sql.{Connection, DriverManager, ResultSet}

class ExecuteQuery(query: String = "", isSelect: Boolean = true) {

  // connect to the database named "mysql" on the localhost
  //val driver = "com.mysql.jdbc.Driver"
  val driver = "com.mysql.cj.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/analysethat"
  val username = "analysethat_admin"
  val password = "password1234"

  // there's probably a better way to do this
  var connection:Connection = null
  var resultSet:ResultSet = null

  //val startQ = System.currentTimeMillis
  //println("Executing query...")

  try {
    // make the connection
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)

    // create the statement, and run the select query
    val statement = connection.createStatement()
    if(isSelect){
      val result = statement.executeQuery(query)
      resultSet = result
    }else{
      statement.executeUpdate(query)
    }
    //val durationQ = (System.currentTimeMillis - startQ)
    //println(s"Query executed. Execution time = $durationQ ms")
  } catch {
    case e: Throwable => e.printStackTrace
      println("Error Executing Query")
  }
  //end Execute Query
}

