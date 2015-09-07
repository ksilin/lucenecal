import java.nio.file.{Files, Path}

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.codecs.simpletext.SimpleTextCodec
import org.apache.lucene.document._
import org.apache.lucene.facet.FacetsConfig
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler
import org.apache.lucene.search._
import org.apache.lucene.store.{Directory, FSDirectory}
import org.scalatest._

class DisjunctionSearchSpec extends FunSpec with Matchers with SpecHelper with BeforeAndAfter {

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
          println(s"id = $id, score = ${sd.score})}")
          // TODO - facets?
        }
      }

      {
        val topdocs = is.searchAfter(lastDoc, q, 2)
        println(s"PAGE 2: topdocs.totalHits = ${topdocs.totalHits}, max score = ${topdocs.getMaxScore}")
        topdocs.scoreDocs.foreach { sd ⇒
          val document: Document = is.doc(sd.doc)
          val id: String = document.get("id")
          println(s"id = $id, score = ${sd.score}")
        }
      }
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

  val doc2 = new Document()
  doc2.add(new StringField("id", "5678", Field.Store.YES))
  doc2.add(new TextField("content", "ipsum sit, AMET elit lorem.", Field.Store.YES))
  doc2.add(new TextField("content2", "foo bar", Field.Store.YES))

  val doc3 = new Document()
  doc3.add(new StringField("id", "9012", Field.Store.YES))
  doc3.add(new TextField("content", "One morning, when Gregor Samsa woke from troubled dreams, he found himself transformed in his bed into a horrible vermin.", Field.Store.YES))
  doc3.add(new TextField("content2", "boo boo bla bla", Field.Store.YES))
}
