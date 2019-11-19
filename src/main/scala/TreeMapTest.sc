import scala.collection.mutable


//implicit val ordering = new Ordering[Int] {
//  override def compare(x: Int, y: Int) = {
//    y - x
//  }
//}

val treeMap = mutable.TreeMap.empty[Int, Int]
treeMap.put(1, 100)
treeMap.put(5, 500)
treeMap.put(2, 10000)

println(treeMap.toSeq.sortWith(_._2 > _._2))
