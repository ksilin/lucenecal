import java.io.Reader

import com.example.{LogEvent, LogLineParser, Lucenecul}
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents
import org.apache.lucene.analysis.{Analyzer, TokenStream}
import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter
import org.apache.lucene.analysis.core.{LowerCaseFilter, StopFilter}
import org.apache.lucene.analysis.custom.CustomAnalyzer
import org.apache.lucene.analysis.custom.CustomAnalyzer.Builder
import org.apache.lucene.analysis.miscellaneous.LengthFilter
import org.apache.lucene.analysis.ngram.NGramTokenFilter
import org.apache.lucene.analysis.standard.{StandardAnalyzer, StandardTokenizer}
import org.apache.lucene.analysis.tokenattributes.{CharTermAttribute, OffsetAttribute, PositionIncrementAttribute}
import org.apache.lucene.analysis.util.CharArraySet
import org.apache.lucene.codecs.simpletext.SimpleTextCodec
import org.apache.lucene.document._
import org.apache.lucene.facet.FacetsConfig
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.lucene.util.GeoDistanceUtils
import org.scalatest.{FunSpec, Matchers}

import scala.collection.GenTraversableOnce
import scala.collection.JavaConverters._
import scala.io.Source

class CustomAnalyzerSpec extends FunSpec with Matchers {


  describe("Lucenecul") {

    val lines = Source.fromInputStream(getClass.getResourceAsStream("/import_prh_start_stop.log")).getLines()
    val events: Iterator[Option[LogEvent]] = lines map LogLineParser.lineToLogEvent
    val docs: GenTraversableOnce[Document] = events.flatten.map(Lucenecul.toDocument)

    it("geo") {
      val distance: Double = GeoDistanceUtils.vincentyDistance(51.0, 13.37, 51.0, 13.0)
      println(s" $distance")
    }

    Lucenecul.indexDocs(docs)

    it("should be able to search documents") {
      val ipsum: String = "Lorem ipsum dolor sit amet, stop1 stop2 <aa>cAntAnt</aa> <bb>BBBfffasd</bb> <cc>CCCCCSDASDA</cc> consectetur adipiscing elit."
      //        "Sed nisi. Nulla quis sem at nibh elementum imperdiet. Duis sagittis ipsum. Praesent mauris. Fusce nec tellus sed augue semper porta. Mauris massa. Vestibulum lacinia arcu eget nulla. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Curabitur sodales ligula in libero. Sed dignissim lacinia nunc. Curabitur tortor. "

      val ana: CustomAnalyzer = buildAnalyzer
      //      val ana: Analyzer = new MyAnalyzer2

      val ts: TokenStream = ana.tokenStream("", ipsum)

      val termAtt = ts.addAttribute(classOf[CharTermAttribute])
      val offsetAtt = ts.addAttribute(classOf[OffsetAttribute])
      val posIncAtt = ts.addAttribute(classOf[PositionIncrementAttribute])


      ts.reset()
      try {

        while (ts.incrementToken()) {

          val currentToken: String = termAtt.toString
          val offsetStart: Int = offsetAtt.startOffset()
          val offsetEnd: Int = offsetAtt.endOffset()
          val increment: Int = posIncAtt.getPositionIncrement
          println(s" $currentToken, $offsetStart, $offsetEnd, $increment")
        }
      } finally {
        ts.close()
      }

    }

  }

  class MyAnalyzer2 extends Analyzer {

    import scala.collection.JavaConverters._

    def createComponents(fieldName: String): TokenStreamComponents = {

      val tokenizer = new StandardTokenizer()
      tokenizer.setMaxTokenLength(900)

      val stopwords = Set("stop1", "stop2").asJava
      val stopWordSet = CharArraySet.copy(stopwords)

      // stopwords filter would not filter out stop1 from xstop1x
      // you will need to use an ngram filter
      val ngr = new NGramTokenFilter(tokenizer, 6, 6)

      //  new EdgeNGramTokenizer() - often used for autocomplete, starts from the begnning only, while a regular NGrams start with each letter

      val myTokenFilter = new StopFilter(tokenizer, stopWordSet)

      val myLCFilter = new LowerCaseFilter(tokenizer)

      val myTokenFilter2 = new LengthFilter(myTokenFilter, 0, 2000)

      // here the chain is created
      val components: TokenStreamComponents = new TokenStreamComponents(tokenizer, myTokenFilter2)
      components
    }

    // applied before TokenStream is created
    override def initReader(fieldNme: String, reader: Reader): Reader = {
      // tags aa & bb are legal. All other tag names are eliminated
      // the contents of those tags are kept regardless
      new HTMLStripCharFilter(reader, Set("bb", "aa").asJava)
    }

  }


  def buildAnalyzer: CustomAnalyzer = {
    val builder: Builder = CustomAnalyzer.builder()
    builder.withTokenizer("standard", "maxTokenLength", "900")
    builder.addCharFilter("htmlstrip", "escapedTags", "aa, bb")
    builder.addTokenFilter("lowercase")
    builder.addTokenFilter("stop", scala.collection.mutable.Map("words" -> "milk_de.txt", "format" -> "snowball").asJava)
    builder.addTokenFilter("length", "min", "0", "max", "2000")
    builder.build()
  }

  def prepareIndex: FacetsConfig = {
    val iwc = new IndexWriterConfig(new StandardAnalyzer())
      .setCodec(new SimpleTextCodec)
      .setUseCompoundFile(false)
      .setOpenMode(IndexWriterConfig.OpenMode.CREATE)

    val iw = new IndexWriter(Lucenecul.dir, iwc)

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
  doc.add(new TextField("_all", "Lorem ipsum dolor sit amet, AMET lorem elit.", Field.Store.NO))
  doc.add(new TextField("content2", "bar bax", Field.Store.YES))
  doc.add(new TextField("_all", "bar bax", Field.Store.NO))
  doc.add(new IntField("popularity", 42, Field.Store.YES))
  doc.add(new NumericDocValuesField("popularity", 14))
  doc.add(new SortedSetDocValuesFacetField("foo", "baz"))
  doc.add(new SortedSetDocValuesFacetField("foo2", "bar2"))

  val doc2 = new Document()
  doc2.add(new StringField("id", "5678", Field.Store.YES))
  doc2.add(new TextField("content", "ipsum sit, AMET elit lorem.", Field.Store.YES))
  doc2.add(new TextField("_all", "ipsum sit, AMET elit lorem.", Field.Store.NO))
  doc2.add(new TextField("content2", "foo bar", Field.Store.YES))
  doc2.add(new TextField("_all", "foo bar", Field.Store.NO))
  doc2.add(new IntField("popularity", 13, Field.Store.YES))
  doc2.add(new NumericDocValuesField("popularity", 42))
  doc2.add(new SortedSetDocValuesFacetField("foo", "baz"))
  doc2.add(new SortedSetDocValuesFacetField("foo2", "baz2"))

  val doc3 = new Document()
  doc3.add(new StringField("id", "9012", Field.Store.YES))
  doc3.add(new TextField("content", "One morning, when Gregor Samsa woke from troubled dreams, he found himself transformed in his bed into a horrible vermin.", Field.Store.YES))
  doc3.add(new TextField("_all", "One morning, when Gregor Samsa woke from troubled dreams, he found himself transformed in his bed into a horrible vermin.", Field.Store.NO))
  doc3.add(new TextField("content2", "boo boo bla bla", Field.Store.YES))
  doc3.add(new TextField("_all", "boo boo bla bla", Field.Store.NO))
  doc3.add(new IntField("popularity", 27, Field.Store.YES))
  doc3.add(new NumericDocValuesField("popularity", 27))
  doc3.add(new SortedSetDocValuesFacetField("foo", "qux"))
  doc3.add(new SortedSetDocValuesFacetField("foo2", "qux2"))

}
