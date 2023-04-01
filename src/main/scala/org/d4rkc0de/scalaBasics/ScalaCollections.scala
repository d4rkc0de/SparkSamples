import java.util.Scanner

object ScalaCollections extends App {
  val sc = new Scanner(System.in)

  def nextInt(): Int = sc.nextInt()

  def next(): String = sc.next()

  def na(n: Int): Array[Int] = {
    var arr: Array[Int] = Array()
    for (i <- 1 to n) arr = arr :+ nextInt()
    arr
  }

  testGroupBy2()


  def testMap() = {
    var map: Map[String, Int] = Map()
    val n = nextInt()
    for (x <- 1 to n) {
      var s = sc.next()
      map += (s -> (map.getOrElse(s, 0) + 1))
    }
    println(map)
  }

  def testArray() = {
    var arr: Array[Int] = Array()
    val n = nextInt()
    for (i <- 1 to n) {
      var x = nextInt()
      arr = arr :+ x
    }
    arr.foreach(x => print(x + " "))
  }

  def testList() = {
    var list: List[Int] = List()
    val n = nextInt()
    for (i <- 1 to n) {
      var x = nextInt()
      list = list ++ List(x)
    }
    list.foreach(x => print(x + " "))
  }

  def testGroupBy1() = {
    val gb = na(5).groupBy(identity)
    val res = gb.mapValues(x => x.length)
    println(res)
  }

  def testGroupBy2() = {
    val donuts: Seq[(String, Double)] = Seq(("a", 1), ("b", 2) , ("c", 3), ("d", 4), ("c", 5), ("a", 6))
    val donutsGroup: Map[String, Double] = donuts.groupBy(_._1).mapValues(_.map(_._2).sum)
    println(donutsGroup)
  }

  def testSet() = {
    var set: Set[String] = Set()
    for (i <- 1 to 5) set = set ++ Set(next())
    set.foreach(println)
  }

  def findSmallestInterval(numbers: Array[Int]): Int = {
    var ans = Math.abs(numbers(0) - numbers(numbers.length - 1))
    for (i <- 1 until numbers.length) {
      ans = Math.min(ans, Math.abs(numbers(i) - numbers(i - 1)))
    }
    return ans
  }
}
