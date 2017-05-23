package my.org

import java.io.{BufferedWriter, FileWriter, PrintWriter}
import java.text.SimpleDateFormat

import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object LogAnalyzer extends App {
  val inputFile = args(0)
  val outputDir = args(1)

  if (!FileUtils.getFile(inputFile).exists()) {
    throw new IllegalArgumentException("Error! Input file does not exist.")
  }

  FileUtils.cleanDirectory(FileUtils.getFile(outputDir))

  // set logger level to ERROR
  Logger.getLogger("org").setLevel(Level.ERROR)

  // initialize spark context
  val sparkConf = new SparkConf().setAppName("Apache Log Analyzer")
  val sc = new SparkContext(sparkConf)
  var parser = new LogParser
  val out = new PrintWriter(new BufferedWriter(new FileWriter(outputDir + "/analysis.log")))
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

  // create the base RDD containing log entries going to be processed
  val accessLogs = sc.textFile(inputFile)
    .map(parser.parseLine)
    .filter(_ != null)
    .cache()

  val dailyLogs = accessLogs
    .map(entry => (dateFormat.format(entry.timestamp), entry))
    .cache()

  // [(2004-03-07, entry1), (2004-03-07, entry2), ....]

  val dailyVisits = dailyLogs
    .map(t => (t._1, t._2.host))
    .distinct
    .map(t => (t._1, 1L))
    .reduceByKey(_ + _)
    .sortByKey()
    .take(10)

  val dailyPages = dailyLogs
    .map(t => (t._1, t._2.path))
    .filter(t => FilenameUtils.getExtension(t._2).equals(""))
    .map(t => (t._1, 1L))
    .reduceByKey(_ + _)
    .sortByKey()
    .take(10)

  val dailyHits = dailyLogs
    .map(t => (t._1, 1L))
    .reduceByKey(_ + _)
    .sortByKey()
    .take(10)

  val dailyBandwidth = dailyLogs
    .map(t => (t._1, t._2.contentSize))
    .reduceByKey(_ + _)
    .sortByKey()
    .take(10)

  out.println()
  out.println("Daily Statistics")

  for (i <- dailyVisits.indices) {
    out.println("\t%s: %10s visits %10s page views %10s hits %10s".format(
      dailyVisits(i)._1,
      dailyVisits(i)._2,
      dailyPages(i)._2,
      dailyHits(i)._2,
      FileUtils.byteCountToDisplaySize(dailyBandwidth(i)._2)
    ))
  }

  val staticFileTypes = Array("js", "css", "png", "gif", "jpg", "pdf", "txt", "html", "ico")

  val fileTypes = accessLogs
    .map(entry => FilenameUtils.getExtension(entry.path))
    .filter(staticFileTypes.contains(_))
    .cache()

  val totalFileTypeCount = fileTypes.count()

  val fileTypesStats = fileTypes
    .map(_ -> 1L)
    .reduceByKey(_ + _)
    .map(t => (t._1, t._2, t._2.toDouble / totalFileTypeCount * 100)) // (pdf, 1000, 20%)
    .sortBy(_._2)
    .take(10)

  out.println()
  out.println("File type statistics")
  fileTypesStats.foreach((x) => out.println("\t%5s: %10s hits %10.2f %%".format(x._1, x._2, x._3)))

  out.close()
  sc.stop()
}
