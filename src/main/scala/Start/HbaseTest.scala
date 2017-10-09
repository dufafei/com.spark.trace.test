package Start

import Util.HbaseUtil

object HbaseTest {
  def main(args: Array[String]): Unit = {
    HbaseUtil.createTable("test4",Array("column1","column2"),4)
  }
}