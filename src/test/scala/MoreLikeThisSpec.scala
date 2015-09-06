import java.nio.file.{Files, Path}

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.codecs.simpletext.SimpleTextCodec
import org.apache.lucene.document._
import org.apache.lucene.facet.sortedset.{DefaultSortedSetDocValuesReaderState, SortedSetDocValuesFacetCounts, SortedSetDocValuesFacetField, SortedSetDocValuesReaderState}
import org.apache.lucene.facet.{DrillDownQuery, DrillSideways, FacetsCollector, FacetsConfig}
import org.apache.lucene.index.{DirectoryReader, IndexReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.queries.mlt.MoreLikeThis
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler
import org.apache.lucene.search._
import org.apache.lucene.store.{Directory, FSDirectory}
import org.scalatest.{BeforeAndAfter, Matchers, FunSpec}

import scala.collection.JavaConverters._

class MoreLikeThisSpec extends FunSpec with Matchers with SpecHelper with BeforeAndAfter {

  var ir: DirectoryReader = _
  var is: IndexSearcher = _
  var facConf: FacetsConfig = _
  private val tempDir: Path = Files.createTempDirectory("lucene_")
  val dir: Directory = FSDirectory.open(tempDir)

  describe("should perform a more like this search") {

    it("first failed attempt") {
      println("---")

      val mlt = new MoreLikeThis(ir)

      val exampleDocId: Int = 1
      val mltq = mlt.like(exampleDocId)
      val topDocs: TopDocs = is.search(mltq, 10)

      println(s"mltq docs count: ${topDocs.scoreDocs.size}")
      topDocs.scoreDocs.map { sd => println(sd.doc) }
      topDocs.scoreDocs.size should be > 0
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
  doc.add(new TextField("content", "Lorem ipsum dolor sit amet, AMET lorem elit.", Field.Store.YES))
  doc.add(new TextField("content2", "bar bax", Field.Store.YES))

  val doc2 = new Document()
  doc2.add(new StringField("id", "5678", Field.Store.YES))
  doc2.add(new TextField("content", "Lorem ipsum dolor sit amet, AMET lorem elit.", Field.Store.YES))
  doc2.add(new TextField("content2", "foo bar", Field.Store.YES))

  val doc3 = new Document()
  doc3.add(new StringField("id", "9012", Field.Store.YES))
  doc3.add(new TextField("content", "One morning, when Gregor Samsa woke from troubled dreams, he found himself transformed in his bed into a horrible vermin.", Field.Store.YES))
  doc3.add(new TextField("content2", "boo boo bla bla", Field.Store.YES))

}
