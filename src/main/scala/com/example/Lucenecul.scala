package com.example

import java.nio.file.{Path, Files}
import java.time.Instant

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.codecs.Codec
import org.apache.lucene.codecs.simpletext.SimpleTextCodec
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document.{Document, Field, StringField, TextField}
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index._
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.queryparser.classic.QueryParser.Operator
import org.apache.lucene.search._
import org.apache.lucene.store.{FSDirectory, Directory, RAMDirectory}

import scala.io.Source
import scalaj.http.{Http, HttpResponse}

case class LogEvent(val eventTime: Instant, val level: String, val message: String)

object Lucenecul extends Logging {

  private val tempDir: Path = Files.createTempDirectory("lucene_")

  val dir: Directory = FSDirectory.open(tempDir) // new RAMDirectory()

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

    // take a look at a PerFieldAnalyzerWrapper - will have to override some anaylzing stuff

    val iwc: IndexWriterConfig = new IndexWriterConfig(analyzer)
    iwc.setCodec(new SimpleTextCodec())
    iwc.setUseCompoundFile(false) // separate files are better for indexing. Using the compond difle is better for searhc

    // commitPoints can theoretically move through history
    // IndexDeletionPolicy - standard - deletes former commitpoints so you cant go back

    iwc.setOpenMode(OpenMode.CREATE) //_OR_APPEND is the default value)

    val writer: IndexWriter = new IndexWriter(dir, iwc)

    val docs = getDocs(logfilePath)

    if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
      println("adding documents with changelog info")
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

    // if you give it an IndexWriter instead of directory, you get an NRT reader
    // so you dont have to to call close on the writer to receive fresh docs
    val reader: IndexReader = DirectoryReader.open(dir) //FSDirectory.open(Paths.get(PATH_TO_INDEX)))
    // LeafReader or CompositeREader
    // most interesting stuff is contained in the LeafReader


    // pst is the actual index (postings)
    // stringfield does not count frequencies and position (would always be 1 and 0)

    // you can also pass a Thread pool
    val searcher: IndexSearcher = new IndexSearcher(reader)

    // indexing and serach analyzers have to match in order to find relevant stff

    val analyzer: Analyzer = new StandardAnalyzer()

    // StandardQueryParser is the new one, ClassicQueryParser is the old one
    val parser: QueryParser = new QueryParser(field, analyzer)
    // default is OR
    parser.setDefaultOperator(Operator.AND)

    // default is false
    parser.setLowercaseExpandedTerms(true)

    // things we missed - faceting, ighlighting, explaining
    // scores of a query are unique to this query and are not comparable between queries


    val query: Query = parser.parse(field + ":" + value)

    val results: TopDocs = searcher.search(query, limit)

    // todo - print this out
    val explain: Explanation = searcher.explain(query, limit)
    //    explain.

    val hits: Array[ScoreDoc] = results.scoreDocs
    val numTotalHits = results.totalHits
    log.info(numTotalHits + " total matching documents")

    hits.map(num => searcher.doc(num.doc))
  }

  // hunspellfilter for spelling correctionj


  def search2() = {

    val reader: IndexReader = DirectoryReader.open(dir) //FSDirectory.open(Paths.get(PATH_TO_INDEX)))


    // you can also pass a Thread pool
    val searcher: IndexSearcher = new IndexSearcher(reader)

    val mergedFieldInfos: FieldInfos = MultiFields.getMergedFieldInfos(reader)


    // WildcardQuery - runs through teh terms, and rewrites itself in terms of the matches as a BooleanQuery
    // leading wildcard is expensive because it has to traverse everyting
    // leading wildcards are so expensive that there is an extras setting of that - setAllwoLeadingWildcard
    val topDocs: TopDocs = searcher.search(new TermQuery(new Term("content", "milk")), 10)

    topDocs.scoreDocs.foreach { d =>
      val dc: Document = searcher.doc(d.doc)
      println(dc.getField("content"))
    }
  }

}
