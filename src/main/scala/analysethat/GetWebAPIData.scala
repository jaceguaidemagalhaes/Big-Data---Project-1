package analysethat
import scala.sys.process._
class GetWebAPIData(){
  var path = System.getProperty("user.dir")+"/webAPIData"
  var pathZip = System.getProperty("user.dir")+"/webAPIData/nfl-scores-and-betting-data.zip"

//  dowloadFilesFromWeb()
//  unzipFiles()

  def dowloadFilesFromWeb(): Unit = {
    try{
    println("Downloading files from web....")
    s"kaggle datasets download --force tobycrabtree/nfl-scores-and-betting-data -p $path"!
  }catch {
    case e: Throwable => e.printStackTrace
    println("Error reading source files from web.")
  }
  }

  def unzipFiles(): Unit = {
    try{
      println("Unziping files from web....")
      s"unzip -o $pathZip -d $path"!
    }catch {
      case e: Throwable => e.printStackTrace
        println("Error reading source files from web.")
    }
  }

}
