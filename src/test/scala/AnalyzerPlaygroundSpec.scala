import org.apache.lucene.analysis.de.GermanAnalyzer
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.scalatest.{FunSpec, Matchers}
import resource._

import scala.io.Source

class AnalyzerPlaygroundSpec extends FunSpec with Matchers {

  describe("Analyzers") {

    val tetris_txt: String = Source.fromFile(getClass.getResource("/new_tetris_en.txt").getFile).mkString
    val milk_en: String = Source.fromFile(getClass.getResource("/milk_en.txt").getFile).mkString
    val milk_de: String = Source.fromFile(getClass.getResource("/milk_de.txt").getFile).mkString

    it("should tokenize text dropping stopwords") {
      for {
        ana <- managed(new StandardAnalyzer())
        ts <- managed(ana.tokenStream("content", tetris_txt))
      } {
        val chars: CharTermAttribute = ts.addAttribute(classOf[CharTermAttribute])
        ts.reset()

        while (ts.incrementToken()) {
          println(s"[$chars]")
        }
      }
    }

    it("should tokenize text dropping german stopwords") {
      for {
        anaEn <- managed(new EnglishAnalyzer())
        anaDe <- managed(new GermanAnalyzer())
        ts <- managed(anaEn.tokenStream("content", milk_en))
        tsDe <- managed(anaDe.tokenStream("content", milk_en))
      } {
        val chars: CharTermAttribute = ts.addAttribute(classOf[CharTermAttribute])
        ts.reset()

        while (ts.incrementToken()) {
          print(s", [$chars]")
        }

        println()
        val charsDe: CharTermAttribute = tsDe.addAttribute(classOf[CharTermAttribute])
        tsDe.reset()

        // here you can see how engliosh stopwords are not removed
        while (tsDe.incrementToken()) {
          print(s", [$charsDe]")
        }
      }

      for {
        anaDe <- managed(new GermanAnalyzer())
        ts <- managed(anaDe.tokenStream("content", milk_de))
      } {
        val chars: CharTermAttribute = ts.addAttribute(classOf[CharTermAttribute])
        ts.reset()

        while (ts.incrementToken()) {
          print(s", [$chars]")
        }
      }
    }
  }

}
