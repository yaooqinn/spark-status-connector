package org.apache.spark.yao.util
import java.util
import java.util.{HashMap => JHashMap, Locale, Map => JMap, Set => JSet}

class CaseInsensitiveMap[V] extends JMap[String, V] {
  private val delegate = new JHashMap[String, V]()

  private def toLowerCase(key: Any): String = key.toString.toLowerCase(Locale.ROOT)

  override def containsKey(key: Any): Boolean = delegate.containsKey(toLowerCase(key))

  override def containsValue(value: Any): Boolean = delegate.containsValue(value)

  override def get(key: Any): V = delegate.get(toLowerCase(key))

  override def remove(key: Any): V = delegate.remove(toLowerCase(key))

  override def putAll(m: JMap[_ <: String, _ <: V]): Unit = {
    m.forEach { (k, v) => delegate.put(toLowerCase(k), v)}
  }

  override def keySet(): JSet[String] = delegate.keySet()

  override def values(): util.Collection[V] = delegate.values()

  override def entrySet(): JSet[JMap.Entry[String, V]] = delegate.entrySet()

  override def size(): Int = delegate.size()

  override def isEmpty: Boolean = delegate.isEmpty

  override def put(key: String, value: V): V = delegate.put(toLowerCase(key), value)

  override def clear(): Unit = delegate.clear()

  def apply(key: String): V = get(key)
}
