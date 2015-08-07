import java.time.Instant

import com.example.Lucenecal
import org.scalatest._

class LucenecalSpec extends FunSpec with Matchers {

  describe("Lucenecal") {
    it("should have tests") {
      val item: Array[String] = Lucenecal.splitAndClean("[2015-07-24T18:07:36,766Z] [INFO ] [SagImport] - importing table ARTISPERREGEL from system PRH")
      println(item)
    }

    it("should parse dates"){
      val instant: Instant = Instant.parse("2015-07-24T18:07:36.766Z")
      println(instant.toString)
    }
  }



}
