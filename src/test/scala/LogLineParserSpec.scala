import java.time.Instant

import com.example.{LogEvent, LogLineParser}
import org.scalatest._

class LogLineParserSpec extends FunSpec with Matchers {

  describe("LogLineParser") {

    val line: String = "[2015-07-24T18:07:36,766Z] [INFO ] [SagImport] - importing table ARTISPERREGEL from system PRH"

    it("should find and parse the date") {
      val maybeEvent: Option[LogEvent] = LogLineParser.lineToLogEvent(line)
      maybeEvent.get.eventTime shouldBe Instant.parse("2015-07-24T18:07:36.766Z")
      maybeEvent.get.level shouldBe "INFO"
      maybeEvent.get.message shouldBe "importing table ARTISPERREGEL from system PRH"
    }
  }

}
