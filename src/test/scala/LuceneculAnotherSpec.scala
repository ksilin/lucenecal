import java.nio.file.{Files, Path}

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.codecs.simpletext.SimpleTextCodec
import org.apache.lucene.document._
import org.apache.lucene.facet.DrillSideways.DrillSidewaysResult
import org.apache.lucene.facet.sortedset.{DefaultSortedSetDocValuesReaderState, SortedSetDocValuesFacetCounts, SortedSetDocValuesFacetField, SortedSetDocValuesReaderState}
import org.apache.lucene.facet.{DrillDownQuery, DrillSideways, FacetsCollector, FacetsConfig}
import org.apache.lucene.index.{DirectoryReader, IndexReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.queries.mlt.MoreLikeThis
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler
import org.apache.lucene.search._
import org.apache.lucene.store.{Directory, FSDirectory}
import org.scalatest._

import scala.collection.JavaConverters._

class LuceneculAnotherSpec extends FunSpec with Matchers with SpecHelper with BeforeAndAfter {

  var ir: DirectoryReader = _
  var is: IndexSearcher = _
  var facConf: FacetsConfig = _
  private val tempDir: Path = Files.createTempDirectory("lucene_")
  val dir: Directory = FSDirectory.open(tempDir)

  before {
    facConf = prepareIndex
    ir = DirectoryReader.open(dir)
    is = new IndexSearcher(ir)
  }

  after {
    ir.close()
  }


  describe("drill sideways") {

    it("should do a drill sideways search") {

      val baseQuery = new MatchAllDocsQuery
      val dq = new DrillDownQuery(facConf, baseQuery)
      dq.add("foo", "baz")

      // a DrillSidewaysQuery seems to combine several DrillDownQueries
      //      Query baseQuery, Collector drillDownCollector, Collector[] drillSidewaysCollectors, Query[] drillDownQueries, boolean scoreSubDocsAtOnce
      //      val dq = new DrillSidewaysQuery(...)

      val descending = true
      val sortByPopularity = new Sort(new SortField("popularity", SortField.Type.INT, descending))
      // multi select facet
      val fillFields = true
      val trackDocScores = true
      val trackMaxScore = true
      val hitsCollector = TopFieldCollector.create(sortByPopularity, 2, fillFields, trackDocScores, trackMaxScore)

      // what is the role of this State again?
      // impl detail of Lucene - reads doc values
      val ssdvrs = new DefaultSortedSetDocValuesReaderState(ir)
      val ds = new DrillSideways(is, facConf, ssdvrs)

      // DrillSideways.search requires a DrillDownQuery, not a DrillSidewaysQuery, a bit counterintuitive
      val dsResult: DrillSidewaysResult = ds.search(dq, hitsCollector)

      val topdocs = hitsCollector.topDocs()
      println(s"topdocs: totalHits = ${topdocs.totalHits}, max score = ${topdocs.getMaxScore}")
      topdocs.scoreDocs.foreach { sd ⇒
        val document: Document = is.doc(sd.doc)
        val id: String = document.get("id")
        println(s"id = $id, score = ${sd.score}, popularity = ${document.get("popularity")}, popularity field = ${document.getField("popularity")}")
      }
      println("result facets: ")
      val facets = dsResult.facets
      facets.getAllDims(10).asScala.foreach(println)

      println("result hits: do they differ from the ones from the collector?")
      val hits: TopDocs = dsResult.hits
      println(s"hits: $hits")
    }

    it("should do am OR / disjunction search") {
      println("---")

      val qp = new StandardQueryParser(new StandardAnalyzer())
      qp.setDefaultOperator(StandardQueryConfigHandler.Operator.OR)
      qp.setLowercaseExpandedTerms(true)

      val fields = Set("content", "content2")
      val queries = fields.map { field ⇒
        qp.parse( """ipsum sit""", field)
      }

      // TODO - would this disjunction query be different?
      // result set is same, scores are different
      //  val q = new DisjunctionMaxQuery(queries.asJavaCollection, 1.0f)

      val boolBuilder = new BooleanQuery.Builder()
      queries foreach { q ⇒
        boolBuilder.add(q, BooleanClause.Occur.SHOULD)
      }
      val q = boolBuilder.build()

      var lastDoc: ScoreDoc = null

      {
        val topdocs = is.search(q, 2)
        println(s"PAGE 1: topdocs.totalHits = ${topdocs.totalHits}, max score = ${topdocs.getMaxScore}")
        topdocs.scoreDocs.foreach { sd ⇒
          lastDoc = sd
          val document: Document = is.doc(sd.doc)
          val id: String = document.get("id")
          println(s"id = $id, score = ${sd.score}, popularity = ${document.getField("popularity")}")
        }
      }

      {
        val topdocs = is.searchAfter(lastDoc, q, 2)
        println(s"PAGE 2: topdocs.totalHits = ${topdocs.totalHits}, max score = ${topdocs.getMaxScore}")
        topdocs.scoreDocs.foreach { sd ⇒
          val document: Document = is.doc(sd.doc)
          val id: String = document.get("id")
          println(s"id = $id, score = ${sd.score}, popularity = ${document.getField("popularity")}")
        }
      }

      ir.close()
    }

    it("should do a more like this search") {
      println("---")

      val mlt = new MoreLikeThis(ir)

      val mltq = mlt.like(1)

      println("mltq: " + mltq)

      val topDocs: TopDocs = is.search(mltq, 1)


      println("mltq top docs: ")
      topDocs.scoreDocs.map { sd => sd.doc }


      val qp = new StandardQueryParser(new StandardAnalyzer())
      qp.setDefaultOperator(StandardQueryConfigHandler.Operator.OR)
      qp.setLowercaseExpandedTerms(true)

      val fields = Set("content", "content2")
      val queries = fields.map { field ⇒
        qp.parse( """ipsum sit""", field)
      }

      val boolBuilder = new BooleanQuery.Builder()
      queries foreach { q ⇒
        boolBuilder.add(q, BooleanClause.Occur.SHOULD)
      }
      val q = boolBuilder.build()

      var lastDoc: ScoreDoc = null

      {
        val topdocs = is.search(q, 2)
        println(s"PAGE 1: topdocs.totalHits = ${topdocs.totalHits}, max score = ${topdocs.getMaxScore}")
        topdocs.scoreDocs.foreach { sd ⇒
          lastDoc = sd
          val document: Document = is.doc(sd.doc)
          val id: String = document.get("id")
          println(s"id = $id, score = ${sd.score}, popularity = ${document.getField("popularity")}")
        }
      }

      {
        val topdocs = is.searchAfter(lastDoc, q, 2)
        println(s"PAGE 2: topdocs.totalHits = ${topdocs.totalHits}, max score = ${topdocs.getMaxScore}")
        topdocs.scoreDocs.foreach { sd ⇒
          val document: Document = is.doc(sd.doc)
          val id: String = document.get("id")
          println(s"id = $id, score = ${sd.score}, popularity = ${document.getField("popularity")}")
        }
      }

      ir.close()
    }

    // there can be only one combination of directory and indexwriter
    // indexwrier is threadsafe though and can be shared
    it("expensive stuffz") {
      println("---")
      // facets config sould be reused as well

      // document and fields can be reused

      // you dont want to commit and close the IWriter - should remain opened at all times,
      // but you can commit

      // creating an index reader is rather expensive. A new one has to be created only once the index has been updated
      // even then, you dont have to recreate it, you can also refresh
      // but in order to refresh properly, you will need to create the IR from an IWriter
      // DirectoryReader.openIfChanged(oldReader) - if no changes have occured, null is returned

      // TODO - try it by creating a reader too early and then refreshing


      // --

      // if you dont commit IWriter, you cannot read indexed docs, except you are using the NRT (near real-time) API
      // you dont always use the NRT api because searching in MEM is a bit more expensive as the structure is not optimized yet

      // a reader refresh also takes some time - you dont want to do it constantly, but scheduled to an interval of some seconds

      // DirectoryReader.open(iw, /* hande deletes in real time (before commit) */)
      // DirectoryReader.openIfChanged(oldReader, iWriter, handleRealtimeDeletes)


      // ---

      // if you create (or refresh) a reader, you will also neeed to recreate the searcher as well as the **State instance
      // but you can also refresh it

      // since you are condemned to having some sort of a hub where the instances live, there are some helpers for that
      // there is a SearcherManager:
      // DH-Lucene creates a FieldSearcherFactory

      // when using a SearcherManager, it becomes as simple as calling searcherManager.acquire()
      // instead of closing the searcher, you can release it: `sManager.release(searcher)`
      // is the end, close the sManager: sm.close()
      // sm.acquire does not perform a refresh. You will have to perform it explicitly: sm.maybeRefresh - blocks while refreshing
      // or sm.refreshBlocking - blokcs UNTIL there is smethign to refresh

      class StateSearchFactory extends SearcherFactory {

        var state: SortedSetDocValuesReaderState = _

        override def newSearcher(reader: IndexReader, previousReader: IndexReader): IndexSearcher = {
          try {
            state = new DefaultSortedSetDocValuesReaderState(reader)
          } catch {
            case e: IllegalArgumentException ⇒
          }
          super.newSearcher(reader, previousReader)
        }
      }

      val iwc = new IndexWriterConfig(new StandardAnalyzer())
        .setCodec(new SimpleTextCodec)
        .setUseCompoundFile(false)
        .setOpenMode(IndexWriterConfig.OpenMode.CREATE)

      val iw = new IndexWriter(dir, iwc)

      val sf = new StateSearchFactory

      val sm = new SearcherManager(iw, true, sf)

      val fc = new FacetsConfig
      fc.setMultiValued("foo", true)

      iw.addDocument(fc.build(doc))
      iw.addDocument(fc.build(doc2))
      iw.addDocument(fc.build(doc3))

      println(s"sm.maybeRefresh() = ${timed("refresh")(sm.maybeRefresh())}")

      val qp = new StandardQueryParser(new StandardAnalyzer())
      qp.setDefaultOperator(StandardQueryConfigHandler.Operator.OR)
      qp.setLowercaseExpandedTerms(true)

      val baseQuery = new MatchAllDocsQuery
      val dq = new DrillDownQuery(fc, baseQuery)
      dq.add("foo", "baz")

      val descending = true
      val sortByPopularity = new Sort(new SortField("popularity", SortField.Type.INT, descending))

      var lastDoc: ScoreDoc = null

      {
        val is = timed("page 1: new searcher")(sm.acquire())
        val ssdvrs = timed("page 1: new state")(sf.state)
        // multi select facet
        val hitsCollector = TopFieldCollector.create(sortByPopularity, 2, true, true, true)
        val ds = new DrillSideways(is, fc, ssdvrs)
        val dsResult = ds.search(dq, hitsCollector)
        val facets = dsResult.facets
        val topdocs = hitsCollector.topDocs()

        println(s"PAGE 1: topdocs.totalHits = ${topdocs.totalHits}, max score = ${topdocs.getMaxScore}")
        topdocs.scoreDocs.foreach { sd ⇒
          lastDoc = sd
          val document: Document = is.doc(sd.doc)
          val id: String = document.get("id")
          println(s"id = $id, score = ${sd.score}, popularity = ${document.getField("popularity")}")
        }
        facets.getAllDims(10).asScala.foreach(println)
        sm.release(is)
      }

      {
        // no multi select facet, just drill down
        val is = timed("page 2: new searcher")(sm.acquire())
        val ssdvrs = timed("page 2: new state")(sf.state)
        val fc = new FacetsCollector()
        val topdocs = FacetsCollector.searchAfter(is, lastDoc, dq, 2, sortByPopularity, true, true, fc)
        val facets = new SortedSetDocValuesFacetCounts(ssdvrs, fc)

        println(s"PAGE 2: topdocs.totalHits = ${topdocs.totalHits}, max score = ${topdocs.getMaxScore}")
        topdocs.scoreDocs.foreach { sd ⇒
          val document: Document = is.doc(sd.doc)
          val id: String = document.get("id")
          println(s"id = $id, score = ${sd.score}")
        }
        facets.getAllDims(10).asScala.foreach(println)
        sm.release(is)
      }

      sm.close()
      iw.close()

      // IndexConfig & PErEntitySearchConfig are such hubs containing relevant instances
      // after acquiring the search form the config, the client performs finally ctx.release.run

      // SearcherLifetimeManager - short intro - TODO - try out - create new docs after search and compare result with ltMAn and without
      // lo-ng paged search over a changing index - you can give tokens to searchers
      // request tokens - you can acquire a searcher with a certain tag in order to have a stable longterm search
      // reusees avilable data
      // tiekn = ltMan.record(iSearcher)
      // ltMAn.release(searcher)

      // regaining the searcher over toke:
      //ltMan.acquire(token) // null if not available
      // ltMAn supports pruning ltMan.prune()
      // there is no lifetime pruning in ES and neither in DH-L

      // with taxonomywriter you only index a FacetField, not DocValuesFaetField at index/doc creation time
      // fc.build requires also the taxomonyMAn
      // not creating a state, retrieve a txonomy reader
      // search is a different, you pass the tyxReader insted of the state
      // facets = FastTaxonomyFacetsCounts
      // similar API . very different encoding

    }

  }

  def prepareIndex: FacetsConfig = {
    val iwc = new IndexWriterConfig(new StandardAnalyzer())
      .setCodec(new SimpleTextCodec)
      .setUseCompoundFile(false)
      .setOpenMode(IndexWriterConfig.OpenMode.CREATE)

    val iw = new IndexWriter(dir, iwc)

    val facConf = new FacetsConfig
    facConf.setMultiValued("foo", true)

    iw.addDocument(facConf.build(doc))
    iw.addDocument(facConf.build(doc2))
    iw.addDocument(facConf.build(doc3))
    iw.commit()
    iw.close()
    facConf
  }

  val doc = new Document()
  doc.add(new StringField("id", "1234", Field.Store.YES))
  doc.add(new TextField("content", "Lorem ipsum dolor sit amet, AMET lorem elit.", Field.Store.YES))
  doc.add(new TextField("content2", "bar bax", Field.Store.YES))
  // // required for value display - document.getField("popularity")
  doc.add(new IntField("popularity", 42, Field.Store.YES))
  // required for sorting, otherwise
  // java.lang.IllegalStateException: unexpected docvalues type NONE for field 'popularity' (expected=NUMERIC). Use UninvertingReader or index with docvalues.
  doc.add(new NumericDocValuesField("popularity", 42))
  doc.add(new SortedSetDocValuesFacetField("foo", "baz"))
  doc.add(new SortedSetDocValuesFacetField("foo2", "bar2"))

  val doc2 = new Document()
  doc2.add(new StringField("id", "5678", Field.Store.YES))
  doc2.add(new TextField("content", "ipsum sit, AMET elit lorem.", Field.Store.YES))
  doc2.add(new TextField("content2", "foo bar", Field.Store.YES))
  // required for value display - document.getField("popularity")
  doc2.add(new IntField("popularity", 4, Field.Store.YES))
  // required for sorting, otherwise
  // java.lang.IllegalStateException: unexpected docvalues type NONE for field 'popularity' (expected=NUMERIC). Use UninvertingReader or index with docvalues.
  doc2.add(new NumericDocValuesField("popularity", 4))
  doc2.add(new SortedSetDocValuesFacetField("foo", "baz"))
  doc2.add(new SortedSetDocValuesFacetField("foo2", "baz2"))

  val doc3 = new Document()
  doc3.add(new StringField("id", "9012", Field.Store.YES))
  doc3.add(new TextField("content", "One morning, when Gregor Samsa woke from troubled dreams, he found himself transformed in his bed into a horrible vermin.", Field.Store.YES))
  doc3.add(new TextField("content2", "boo boo bla bla", Field.Store.YES))
  doc3.add(new IntField("popularity", 27, Field.Store.YES))
  doc3.add(new NumericDocValuesField("popularity", 27))
  doc3.add(new SortedSetDocValuesFacetField("foo", "qux"))
  doc3.add(new SortedSetDocValuesFacetField("foo2", "qux2"))

}
