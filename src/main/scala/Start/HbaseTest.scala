package Start

import Util.HbaseUtil

object HbaseTest {
  def main(args: Array[String]): Unit = {
    HbaseUtil.createTable("bulktest",Array("family"),2)
  }
}