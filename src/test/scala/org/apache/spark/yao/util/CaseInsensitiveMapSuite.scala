package org.apache.spark.yao.util

import org.apache.spark.yao.BaseSuite

class CaseInsensitiveMapSuite extends BaseSuite {

  test("CaseInsensitiveMap should handle case insensitivity correctly") {
    val map = new CaseInsensitiveMap[Int]()
    map.put("KeyOne", 1)
    map.put("keytwo", 2)

    assert(map.get("keyone") == 1)
    assert(map.get("KEYTWO") == 2)
    assert(map.containsKey("KEYONE"))
    assert(!map.containsKey("non_existent_key"))
    assert(map.size() == 2)

    map.remove("KEYONE")
    assert(!map.containsKey("keyone"))
    assert(map.size() == 1)
  }

  test("CaseInsensitiveMap should handle putAll correctly") {
    val map1 = new CaseInsensitiveMap[Int]()
    map1.put("A", 1)

    val map2 = new CaseInsensitiveMap[Int]()
    map2.putAll(map1)

    assert(map2.get("a") == 1)
  }

}
