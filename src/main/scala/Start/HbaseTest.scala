package Start

import Util.HbaseUtil

object HbaseTest {

  HbaseUtil.createTable("test",Array("column1","column2"))

}