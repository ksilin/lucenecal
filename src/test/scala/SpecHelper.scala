trait SpecHelper {

  def timed[A](s: String)(x: ⇒ A): A = {
    val start = System.nanoTime()
    try x finally {
      val end = System.nanoTime()
      val ms = (end - start) / 1000000.0
      println(f"$s took [$ms%.3f] ms")
    }
  }

}
