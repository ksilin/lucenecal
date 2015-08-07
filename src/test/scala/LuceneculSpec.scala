import java.time.Instant

import com.example.Lucenecul
import org.apache.lucene.document.Document
import org.scalatest._

class LuceneculSpec extends FunSpec with Matchers {

  describe("Lucenecul") {

    val FILE_PATH = "/home/kostja/Code/Exellio/data/sag_import/logs/import_prh_start_stop.log"
    Lucenecul.prepareIndex(FILE_PATH)

    it("should create an index with documents") {

      val dir = Lucenecul.dir
      val dirFiles: Array[String] = dir.listAll()
      dirFiles.size shouldBe 4 // the page size


    }

    it("should be able to search documents"){
      val limit: Int = 7
      val scoreDocs: Array[Document] = Lucenecul.search("level", "INFO", limit)//(search("*", "*")

      scoreDocs.size shouldEqual limit
      scoreDocs.foreach( sd => sd.get("level") shouldBe "INFO" )
    }

    it("should parse dates"){
      val instant: Instant = Instant.parse("2015-07-24T18:07:36.766Z")
      println(instant.toString)
    }
  }


}
