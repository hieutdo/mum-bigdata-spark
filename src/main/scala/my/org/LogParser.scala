package my.org

import java.text.SimpleDateFormat
import java.util.regex.Pattern
import java.util.{Date, Locale}

class LogParser extends Serializable {
  // 127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048
  private val regex = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)"
  private val pattern = Pattern.compile(regex)
  private val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss ZZ", Locale.ENGLISH)

  def parseDateField(field: String): Date = {
    dateFormat.parse(field)
  }

  def parseLine(line: String): LogEntry = {
    val matcher = pattern.matcher(line)
    if (matcher.find) {
      LogEntry(
        matcher.group(1),
        parseDateField(matcher.group(4)),
        matcher.group(5),
        matcher.group(6),
        matcher.group(7),
        matcher.group(8).toInt,
        matcher.group(9).toLong
      )
    } else {
      null
    }
  }
}
