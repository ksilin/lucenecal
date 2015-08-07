package com.example

import java.time.Instant

object LogLineParser {

  // ^\s*\[(?<datetime>.*?)\]\s*\[(?<level>.*?)\s*\]\s*\[(?<source>.*?)\] - (?<msg>.*)

  val TIME_RX = """^\s*\[(?<datetime>.*?)\]"""
  val LOGLEVEL_RX = """\[(?<level>[A-Z]*?)\s*\]"""
  val SOURCE_RX = """\[(?<source>.*?)\]"""
  val MSG_RX = """(?<msg>.*)"""
  val START_LINE_RX = """starting import into (.*\.)?(?<table>.+)""".r
  val START_LINE_RX_OLD = """importing table (?<table>.*?) from system""".r
  val END_LINE_RX = """inserted (?<count>\d+) datasets into (.*\.)?(?<table>.+)""".r
  val SPLIT_RX = """(\] \[)|(] - )""".r

  val LINE_RX = s"$TIME_RX\\s*$LOGLEVEL_RX\\s*$SOURCE_RX - $MSG_RX".r

  def sanitize(line: String): String = {
    line.replaceAllLiterally(",", ".")
  }

  def lineToLogEvent(line: String): Option[LogEvent] = {

    val sanitized: String = sanitize(line)
    val seq: Option[List[String]] = LINE_RX.unapplySeq(sanitized)

    seq match {
      case Some(list) => {
        val instant: Instant = Instant.parse(list(0))
        Some(LogEvent(instant, list(1), list(3)))
      }
      case None => println(s"line $line does not match the expected regex: $LINE_RX"); None
    }
  }

  def makeLogEvents(iter: Iterator[String]): Iterator[Option[LogEvent]] = iter.map({str => lineToLogEvent(str)})

}
