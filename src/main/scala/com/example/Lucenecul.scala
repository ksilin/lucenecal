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

object Lucenecul extends Logging{

  val FILE_PATH = "/home/kostja/Code/Exellio/data/sag_import/logs/import_prh_start_stop.log"

  val dir: Directory = new RAMDirectory() //FSDirectory.open(Paths.get(PATH_TO_INDEX))

  def getContentIter(path: String) = {
    Source.fromFile(path).getLines()
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
    val iter: Iterator[String] = getContentIter(filePath)
    val events: Iterator[Option[LogEvent]] = LogLineParser.makeLogEvents(iter)
      events.flatten.map(toDocument)
  }

  def prepareIndex(logfilePath: String) = {

    val analyzer: Analyzer = new StandardAnalyzer()
    val iwc: IndexWriterConfig = new IndexWriterConfig(analyzer)
    iwc.setOpenMode(OpenMode.CREATE) //_OR_APPEND)

    val writer: IndexWriter = new IndexWriter(dir, iwc)

    val docs = getDocs(logfilePath)

    if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
      println("adding docuemnts with changelog info")
      docs.foreach({ doc => writer.addDocument(doc) })
    } else {
      log.debug("updating index with current lucene version with changelog info")
      docs.foreach({ doc =>
        writer.updateDocument(new Term("id", doc.get("id")), doc)
      })
    }
    writer.close()
  }

  def search(field: String, value: String, limit: Int = 5) = {

    val reader: IndexReader = DirectoryReader.open(dir) //FSDirectory.open(Paths.get(PATH_TO_INDEX)))
    val searcher: IndexSearcher = new IndexSearcher(reader)

    val analyzer: Analyzer = new StandardAnalyzer()

    val parser: QueryParser = new QueryParser(field, analyzer)

    val query: Query = parser.parse(field + ":" + value)

    val results: TopDocs = searcher.search(query, limit)

    val hits: Array[ScoreDoc] = results.scoreDocs
    val numTotalHits = results.totalHits
    log.info(numTotalHits + " total matching documents")

    hits.map(num => searcher.doc(num.doc))
  }
}
