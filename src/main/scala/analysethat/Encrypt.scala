package analysethat

import java.security.MessageDigest
class Encrypt{
//object Encrypt extends App{
//val password = "123456"
      def Encrypt(password: String):String= {
  val algorithm = "SHA-256"

  var plainText:Array[Byte] = password.getBytes()

  try {
    var digest = MessageDigest.getInstance(algorithm)
    digest.reset()
    digest.update(plainText)
    val encodedPassword = digest.digest()

    var builder = new StringBuilder();
    for (b <- encodedPassword) {
      if ((b & 0xff) < 0x10) {
        builder.append("0")
      }
      builder.append((b & 0xff).toString())
    }
    return builder.toString()
   // println("Plain    : " + password)
   // println("Encrypted: " + builder.toString())
  } catch {
    case e: Throwable => e.printStackTrace()
      return ""
  }
  //end def Encrypt
}

  //end class
}