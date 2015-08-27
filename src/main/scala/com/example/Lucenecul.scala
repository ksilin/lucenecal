package com.example

import java.nio.file.{Files, Path}
import java.time.Instant
import java.util

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.codecs.simpletext.SimpleTextCodec
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document._
import org.apache.lucene.facet.FacetsConfig
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index._
import org.apache.lucene.queries.mlt.MoreLikeThis
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.queryparser.classic.QueryParser.Operator
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler
import org.apache.lucene.search._
import org.apache.lucene.store.{Directory, FSDirectory}

import scala.collection.GenTraversableOnce
import scala.io.Source
import scalaj.http.Http
import scala.collection.JavaConversions._

case class LogEvent(val eventTime: Instant, val level: String, val message: String)

// TODO things we are missing yet - faceting, highlighting
object Lucenecul extends Logging {

  private val tempDir: Path = Files.createTempDirectory("lucene_")
  val dir: Directory = FSDirectory.open(tempDir)
  // new RAMDirectory()
  val writer: IndexWriter = makeIndexWriter

  def getContentIter(path: String) = Source.fromFile(path).getLines()

  def getContentFromUrl(url: String) = Http(url).asString.body

  def toDocument(event: LogEvent): Document = {
    val doc: Document = new Document()
    // stringfield does not count frequencies and position (would always be 1 and 0)
    doc.add(new StringField("id", "doc_" + System.nanoTime(), Field.Store.YES))
    // TODO - how to store and query dates (and perhaps other non-string types like numbers or locations)
    // translate into LongField and use numeric comparisons
    doc.add(new TextField("level", event.level, Field.Store.YES))
    doc.add(new TextField("content", event.message, Store.YES))

    // since 5, IntFields cannot be sorted by, you will need a DocValuesField
    doc.add(new IntField("popularity", 42, Store.YES))
    doc.add(new NumericDocValuesField("popularity", 42)) // not stored per default

    // todo - if you want faceting, you will ahve to add fields as SortedSetDocValueFacetField
    // SSDVFF - indexes a dummy field

    // faceting

    val fx = new FacetsConfig
    fx.setMultiValued("foo", true) // what does this do ?
    // instead fo just adding the docs, use addDcument(fc.build(doc))



    // SortedDocValuesField - look at the index - is it sorted?
    // a map for each distinct value present and then mapd to the document containing this value
    //

    // DocValues (Numeric & Binary) and numeric values can be updated without reindexing

    // depends on teh index codec actually, if you mod the codec to store anything o redis, you could actually cisrcumvent

    // SOLR and ES ahve different faceting impls
    // SOLR - uninverts the index at strartup time (field cache in RAM)
    // ES

    // TODO - add _all field to search in multiple fields
    // TODO - PhraseSearcher - only quarantees the order of terms, no the literal match
    // for phrease search to work i will need to index docs, fileds and positions
    // PS uses TwoPhase iteration - first to determine if all terms exist, then 2nd phase - ordering

// use Store.NO for the _all field - does not mean that the field is not stored. It is and you can still query it, just not retrieve from the index

    // tODO adding multiple fileds with the same name jsut concatenates them

    // elastic has a built-in _all field and type


    doc
  }

  def getDocs(filePath: String): Iterator[Document] = {
    val iter: Iterator[String] = getContentIter(filePath)
    val events: Iterator[Option[LogEvent]] = LogLineParser.makeLogEvents(iter)
    events.flatten.map(toDocument)
  }

  def indexDocs(docs: GenTraversableOnce[Document]) = {

    if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
      docs.foreach({ doc => writer.addDocument(doc) })
    } else {
      log.debug("updating index with changelog info")
      docs.foreach({ doc =>
        writer.updateDocument(new Term("id", doc.get("id")), doc)
      })
    }
    writer.close()
  }

  def makeIndexWriter: IndexWriter = {
    val analyzer: Analyzer = new StandardAnalyzer()

    // take a look at a PerFieldAnalyzerWrapper - will have to override some anaylzing stuff
    val iwc: IndexWriterConfig = new IndexWriterConfig(analyzer)
    iwc.setCodec(new SimpleTextCodec())
    iwc.setUseCompoundFile(false) // separate files are better for indexing. Using the compond difle is better for searhc
    iwc.setOpenMode(OpenMode.CREATE) //_OR_APPEND is the default value)

    // commitPoints can theoretically move through history but you have to an appropriate IndexDeletionPlicy
    // IndexDeletionPolicy - standard - deletes former commitpoints so you cant go back


    new IndexWriter(dir, iwc)
  }

  def search(field: String, value: String, limit: Int = 5) = {

    // if you give it an IndexWriter instead of directory, you get an NRT reader
    // so you dont have to to call close on the writer to receive fresh docs
    val reader: IndexReader = DirectoryReader.open(dir)
    // LeafReader or CompositeREader
    // most interesting stuff is contained in the LeafReader


    // you can also pass a Thread pool
    val searcher: IndexSearcher = new IndexSearcher(reader)

    // indexing and serach analyzers have to match sufficiently in order to find relevant results
    val analyzer: Analyzer = new StandardAnalyzer()

    // StandardQueryParser is the new one, ClassicQueryParser is the old one
    val parser: QueryParser = new QueryParser(field, analyzer)
    // default is OR
    parser.setDefaultOperator(Operator.AND)

    // default is false
    parser.setLowercaseExpandedTerms(true)

    val query: Query = parser.parse(field + ":" + value)

    val results: TopDocs = searcher.search(query, limit)

    val explain: Explanation = searcher.explain(query, limit)
    //    println(explain.getDescription)
    //    println(explain.getValue)
    // TODO - what do I do with the details?
    //    println(explain.getDetails)
    log.info(" --- scores of a query are unique to this query and are not comparable between queries --- ")
    log.info(explain.toString)

    val numTotalHits = results.totalHits
    log.info(s" found $numTotalHits total matching documents")

    val hits: Array[ScoreDoc] = results.scoreDocs
//    hits.map(num => searcher.doc(num.doc))
    results.scoreDocs
  }

  // TODO hunspellfilter for spelling correction


  def searchTerm(field: String, value: String, limit: Int = 5) = {

    val reader: IndexReader = DirectoryReader.open(dir) //FSDirectory.open(Paths.get(PATH_TO_INDEX)))

    // you can also pass a Thread pool
    val searcher: IndexSearcher = new IndexSearcher(reader)

    // TODO - WildcardQuery - runs through the terms, and rewrites itself in terms of the matches as a BooleanQuery
    // leading wildcard is expensive because it has to traverse everyting
    // leading wildcards are so expensive that there is an extras setting of that - setAllwoLeadingWildcard
    val topDocs: TopDocs = searcher.search(new TermQuery(new Term("content", "milk")), 10)

    topDocs.scoreDocs.foreach { d =>
      val dc: Document = searcher.doc(d.doc)
      println(dc.getField("content"))
    }
  }

  def mlt(docid: Int) = {

    val reader: IndexReader = DirectoryReader.open(dir)
    val searcher: IndexSearcher = new IndexSearcher(reader)

    val mlt = new MoreLikeThis(reader)
    val mltq = mlt.like(docid)

    println("mltq: " + mltq)

    searcher.search(mltq, 1)
  }

  def pagedSearch(pageNum: Int = 0, pageSize: Int = 2) = {

    // TODO  search actually retruns a FieldDoc containing additional info related to the sorting of the original query.
    // the sorting has to match between the original query and the paged search

    // ScoreDoc is a simple int / float container. Can be used and reconstructed if no sorting is used


    val reader: IndexReader = DirectoryReader.open(dir)
    val searcher: IndexSearcher = new IndexSearcher(reader)

    val all = new MatchAllDocsQuery

    val topDocs: TopDocs = searcher.search(all, pageSize + pageNum*pageSize) // add page size


    // TODO - use last found doc to page
    // doc ids have to be stable - a merge can reqrite all ids
//    searcher.searchAfter(doc, query, Sort.RELEVANCE, true, true)

    // suppose page size is 2 and we want to have the second page
    topDocs.scoreDocs.drop(pageNum*pageSize)
  }

  def multiSearch(fields: Set[String]) = {

    val reader: IndexReader = DirectoryReader.open(dir)
    val searcher: IndexSearcher = new IndexSearcher(reader)

    // indexing and serach analyzers have to match sufficiently in order to find relevant results
    val analyzer: Analyzer = new StandardAnalyzer()

    // StandardQueryParser is the new one, ClassicQueryParser is the old one
    val parser: StandardQueryParser = new StandardQueryParser(analyzer)
    parser.setDefaultOperator(StandardQueryConfigHandler.Operator.AND)
    parser.setLowercaseExpandedTerms(true)

    val queries = fields map { f =>
      parser.parse("myserachterm", f)
    }
    val boolBuilder = new BooleanQuery.Builder()

    queries.foreach( q =>
        boolBuilder.add(q, BooleanClause.Occur.SHOULD)
    )

  }

  def multiSearch2() = {

    val reader: IndexReader = DirectoryReader.open(dir)
    val searcher: IndexSearcher = new IndexSearcher(reader)

    // indexing and serach analyzers have to match sufficiently in order to find relevant results
    val analyzer: Analyzer = new StandardAnalyzer()

    // StandardQueryParser is the new one, ClassicQueryParser is the old one
    val parser: StandardQueryParser = new StandardQueryParser(analyzer)
    parser.setDefaultOperator(StandardQueryConfigHandler.Operator.AND)
    parser.setLowercaseExpandedTerms(true)

//    val queries = fields map { f =>
//      parser.parse("myserachterm", f)
//    }

    // disjunction stands for OR / SHOULD
    // difference to the boolean query is onyl the scoring
//    val query: DisjunctionMaxQuery = new DisjunctionMaxQuery(queries, 1.0f)
  }


  def printlReaderDetails() = {
    val reader: IndexReader = DirectoryReader.open(dir)

    // sth like a schema
    val mergedFieldInfos: FieldInfos = MultiFields.getMergedFieldInfos(reader)
    mergedFieldInfos.iterator foreach { fi => println(s"${fi.name}") }

    // same strings as in FieldInfo.name
    val iterator: util.Iterator[String] = MultiFields.getFields(reader).iterator()
    iterator foreach println
  }






  // payload - accessed in the scorer, but is not searchable

  // ES uses payload to boost searh results

  // you need termvectors for MLT to work

}
