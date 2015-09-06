import java.nio.file.{Files, Path}

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.codecs.simpletext.SimpleTextCodec
import org.apache.lucene.document._
import org.apache.lucene.facet.DrillSideways.DrillSidewaysResult
import org.apache.lucene.facet.sortedset.{DefaultSortedSetDocValuesReaderState, SortedSetDocValuesFacetField}
import org.apache.lucene.facet.{DrillDownQuery, DrillSideways, FacetsConfig}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.search._
import org.apache.lucene.store.{Directory, FSDirectory}
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}

import scala.collection.JavaConverters._

class DrillSidewaysSpec extends FunSpec with Matchers with SpecHelper with BeforeAndAfter {

  var ir: DirectoryReader = _
  var is: IndexSearcher = _
  var facConf: FacetsConfig = _
  private val tempDir: Path = Files.createTempDirectory("lucene_")
  val dir: Directory = FSDirectory.open(tempDir)

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
  }

  before {
    facConf = prepareIndex
    ir = DirectoryReader.open(dir)
    is = new IndexSearcher(ir)
  }

  after {
    ir.close()
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
  // required for value display - document.getField("popularity")
  doc.add(new IntField("popularity", 42, Field.Store.YES))
  // required for sorting, otherwise
  // java.lang.IllegalStateException: unexpected docvalues type NONE for field 'popularity' (expected=NUMERIC). Use UninvertingReader or index with docvalues.
  doc.add(new NumericDocValuesField("popularity", 42))
  doc.add(new SortedSetDocValuesFacetField("foo", "baz"))

  val doc2 = new Document()
  doc2.add(new StringField("id", "5678", Field.Store.YES))
  doc2.add(new IntField("popularity", 4, Field.Store.YES))
  doc2.add(new NumericDocValuesField("popularity", 4))
  doc2.add(new SortedSetDocValuesFacetField("foo", "baz"))

  val doc3 = new Document()
  doc3.add(new StringField("id", "9012", Field.Store.YES))
  doc3.add(new IntField("popularity", 27, Field.Store.YES))
  doc3.add(new NumericDocValuesField("popularity", 27))
  doc3.add(new SortedSetDocValuesFacetField("foo", "qux"))

}
