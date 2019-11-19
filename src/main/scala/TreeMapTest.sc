import scala.collection.mutable


val treeMap = mutable.TreeMap.empty[Int, Int]
treeMap.put(1, 100)
treeMap.put(5, 500)
treeMap.put(2, 200)

println(treeMap.ordering)
