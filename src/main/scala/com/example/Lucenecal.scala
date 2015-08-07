package com.example

import java.time.Instant

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document.{Document, Field, StringField, TextField}
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index._
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{IndexSearcher, Query, ScoreDoc, TopDocs}
import org.apache.lucene.store.{Directory, RAMDirectory}

import scala.io.Source
import scalaj.http.{Http, HttpResponse}

case class LogEvent(val eventTime: Instant, val level: String, val message: String)

object Lucenecal {

  val PATH_TO_INDEX: String = "spindex"

  val FILE_PATH = "/home/kostja/Code/Exellio/data/sag_import/logs/import_prh_start_stop.log"

  val TIME_RX = """\[(?<datetime>.*?)\]""".r
  val START_LINE_RX = """starting import into (.*\.)?(?<table>.+)""".r
  val START_LINE_RX_OLD = """importing table (?<table>.*?) from system""".r
  val END_LINE_RX = """inserted (?<count>\d+) datasets into (.*\.)?(?<table>.+)""".r
  val SPLIT_RX = """(\] \[)|(] - )""".r

  val dir: Directory = new RAMDirectory() //FSDirectory.open(Paths.get(PATH_TO_INDEX))

  def main(args: Array[String]): Unit = {
    prepareIndex()

    val scoreDocs: Array[Document] = search("*", "*") //search("level", "INFO")

    scoreDocs.foreach({ sd =>
      val doc: Document = sd
//      val values: Seq[String] = doc.getFields().map({ f: IndexableField => f.stringValue() })
//      println(values.mkString(" , "))
      println(s"level: '${doc.get("level")}'")
    })

    println(s"files: ${dir.listAll().mkString(", ")}")

  }

  def getContentIter(path: String) = {
    Source.fromFile(path).getLines()
  }

  def makeLogEvents(iter: Iterator[String]): Iterator[LogEvent] = iter map lineToLogEvent

  def lineToLogEvent(line: String): LogEvent = {
    val tokens: Array[String] = splitAndClean(line)
    val trimmed = tokens.map(_.trim)
    val instant: Instant = Instant.parse("2015-07-24T18:07:36.766Z")
    LogEvent(instant, trimmed(1), trimmed(3))
  }


  def splitAndClean(line: String): Array[String] = {
    val strings: Array[String] = SPLIT_RX.split(line)
    strings.map(s => s.replaceAllLiterally("[", "").replaceAllLiterally(",", "."))
  }


  def getContentFromUrl(url: String) = {
    val response: HttpResponse[String] = Http(url).asString
    response.body
  }

  def toDocument(event: LogEvent): Document = {
    val doc: Document = new Document()
    doc.add(new StringField("id", "doc_" + System.nanoTime(), Field.Store.YES))
    doc.add(new TextField("level", event.level, Field.Store.YES))
    doc.add(new TextField("content", event.message, Store.YES))
    doc
  }

  def getDocs(filePath: String): Iterator[Document] = {
    makeLogEvents(getContentIter(filePath)) map toDocument
  }

  def prepareIndex() = {

    val analyzer: Analyzer = new StandardAnalyzer()
    val iwc: IndexWriterConfig = new IndexWriterConfig(analyzer)
    iwc.setOpenMode(OpenMode.CREATE) //_OR_APPEND)

    val writer: IndexWriter = new IndexWriter(dir, iwc)

    val docs = getDocs(FILE_PATH)

    if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
      println("adding current lucene version with changelog info")
      docs.foreach({ doc => writer.addDocument(doc) })
    } else {
      println("updating index with current lucene version with changelog info")
      docs.foreach({ doc =>
        writer.updateDocument(new Term("id", doc.get("id")), doc)
      })
    }
    writer.close()
  }

  def search(field: String, value: String) = {

    val reader: IndexReader = DirectoryReader.open(dir) //FSDirectory.open(Paths.get(PATH_TO_INDEX)))
    val searcher: IndexSearcher = new IndexSearcher(reader)

    val analyzer: Analyzer = new StandardAnalyzer()

    val parser: QueryParser = new QueryParser(field, analyzer)

    val query: Query = parser.parse(field + ":" + value)

    val results: TopDocs = searcher.search(query, 5)

    val hits: Array[ScoreDoc] = results.scoreDocs
    val numTotalHits = results.totalHits
    println(numTotalHits + " total matching documents")

    if (hits.length > 0) {
      /*
       * Matching score for the first document
       */
      println("Matching score for first document: " + hits(0).score)

      /*
       * We load the document via the doc id to be found in the ScoreDoc.doc attribute
       */
      val doc: Document = searcher.doc(hits(0).doc)
      println("message of the document: " + doc.get("content"))
    }
    hits.map(num => searcher.doc(num.doc))
  }
}
