import java.time.Instant

import com.example.{LogEvent, LogLineParser, Lucenecul}
import org.apache.lucene.document.Document
import org.scalatest._

import scala.collection.GenTraversableOnce
import scala.io.Source

class LuceneculSpec extends FunSpec with Matchers {

  describe("Lucenecul") {

    val lines = Source.fromInputStream(getClass.getResourceAsStream("/import_prh_start_stop.log")).getLines()
    val events: Iterator[Option[LogEvent]] = lines map LogLineParser.lineToLogEvent
    val docs: GenTraversableOnce[Document] = events.flatten.map(Lucenecul.toDocument)

    Lucenecul.indexDocs(docs)

    it("should create an index with position, info, field, writelock and other files") {

      val dir = Lucenecul.dir
      val dirFiles: Array[String] = dir.listAll()

      dirFiles.size shouldBe 7
      val expected = List("_0.len", "_0.pst", "_0.inf", "_0.si", "write.lock", "_0.fld", "segments_1")
      dirFiles should contain theSameElementsAs(expected)
    }

    it("should be able to search documents") {
      val limit: Int = 7
      val scoreDocs: Array[Document] = Lucenecul.search("level", "INFO", limit) //(search("*", "*")

      scoreDocs.size shouldEqual limit
      scoreDocs.foreach(sd => sd.get("level") shouldBe "INFO")
    }

    it("should be able to search documents 2") {
      val results: Unit = Lucenecul.searchTerm("content", "milk")
    }

    it("should parse dates") {
      val instant: Instant = Instant.parse("2015-07-24T18:07:36.766Z")
      println(instant.toString)
    }
  }


}
