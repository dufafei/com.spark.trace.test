package Util

import Init.Pattern
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.log4j.{Level, Logger}
/**
  *
  *为user实例定义一个简单模型对象
  * 在一个类中封装所有Hbase访问方法
  * 先声明普通使用的字节数组byte[]常量
  * 然后定义封装操作命令的方法
  * 接下来是user模型的公有接口和私有实现
  * Created by 小灰灰 on 2017/9/23.
  */
object HbaseUtil extends Pattern{

  val logger=Logger.getLogger(HbaseUtil.getClass)
  logger.setLevel(Level.WARN)

  val hbaseEncode = "utf8"
  //使用admin建表，删表
  def useAdmin[C](execute:Admin => C): C ={
    using(HbaseConn.getAdmin())(execute)
  }
  //使用Table进行CRUD操作
  def useTable[D](execute:Connection => D): D ={
   using(HbaseConn.getConnection())(execute)
  }
  //判断表是否存在
  def tableIsExists(tableName: TableName):Boolean ={
    useAdmin(_.tableExists(tableName))
  }

  /**
    *addFamily添加一个列族
    * @param tableName 表名称
    * @param familyName 列簇名称集合
    */
  def createTable(tableName:String,familyName:Array[String],partition:Int): Unit ={
   useAdmin(x=>{
     val table=TableName.valueOf(tableName)
     if(x.tableExists(table))
       logger.warn(s"create table:$tableName is already exists")
     else {
       val tableDesc=new HTableDescriptor(table)
       familyName.indices.map(x => {
         val family=new HColumnDescriptor(familyName(x))
         tableDesc.addFamily(family)
         //开启压缩
         family.setCompressionType(Algorithm.SNAPPY)
       })
       x.createTable(tableDesc,getSplitKeys(partition))
       println(s"创建表：${tableName}成功")
     }
   })
  }
   //预分区
  /**
    *
    * @param partition 分区个数
    * @return rowkey临界值
    */
  def getSplitKeys(partition:Int) ={
    val splitKeys=new Array[Array[Byte]](partition-1)
    for(i <- 1 until partition){
     splitKeys(i-1)=String.format("%x",i.toString).getBytes
    }
       splitKeys
  }

  def deleteTable(tableName: String): Unit ={
    useAdmin(x =>{
      val table=TableName.valueOf(tableName)
      if (x.tableExists(table)) {
        x.disableTable(table)
        x.deleteTable(table)}
      else
        logger.warn(s"delete table:$tableName is not exists")
    })
  }
  //插入行-put

  def insertLine(tableName:String,rowKey:String,familyName:String,columnName:String,value:String): Unit ={
    useTable(x=>{
      val toTable=TableName.valueOf(tableName)
      val flag=tableIsExists(toTable)
      if(flag){
        val table=x.getTable(toTable)
        val put=new Put(rowKey.getBytes(this.hbaseEncode))
        put.addColumn(familyName.getBytes,columnName.getBytes(),value.getBytes())
        table.setWriteBufferSize(6 * 1024 * 1024)
        table.put(put)
      }
      else
        logger.warn("table is not exists")
    })
  }
  //插入表
  def insertTable(tableName:String,rowkey:String): Unit ={

  }

  //删除行-delete

  def deleteLine(tableName:String,rowkeys:Array[String]): Unit ={
    useTable(x=> {
      val toTable=TableName.valueOf(tableName)
      val flag=tableIsExists(toTable)
      if (flag){
        val table=x.getTable(toTable)
        val deleteList=rowkeys.map(x=>{
          new Delete(x.getBytes(this.hbaseEncode))
          //删除行的一部分，delete.deleteColumn
        }).toList
        import scala.collection.JavaConversions._
        table.delete(deleteList)
      }
     else
        logger.warn("table is not exists")
    })
  }
  //获取行-get
  def getLine(tableName:String,rowkey:String,familyName:String,columnName:String) ={
    useTable(x=>{
      val toTable=TableName.valueOf(tableName)
      val flag=tableIsExists(toTable)
      if (flag){
        val table=x.getTable(toTable)
        val get=new Get(rowkey.getBytes(this.hbaseEncode))
        //get.addColumn(("info").getBytes,"name".getBytes)
        //get.addFamily("info".getBytes)
         //val r=table.get(get)
        //val b=r.getValue("info".getBytes,"name".getBytes)
        //检索特定值，从字节换回字符串
        //val p=b.toString
        val res=table.get(get)
        val cells=res.rawCells()//Array[cell]
        getRow(cells)
      }
      else
        logger.warn("table is not exists")
    })
  }
  //获取单元格的值
  def getRow(cell:Array[Cell]) ={
    cell.indices.map(x=>{
    val rowkey=CellUtil.cloneRow(cell(x))
    val familyName=CellUtil.cloneFamily(cell(x))
    val columnName=CellUtil.cloneQualifier(cell(x))
    val value=CellUtil.cloneValue(cell(x))
      (rowkey,familyName,columnName,value)
    })
  }
  //获取多行-scan
  def getLines(tableName:String,startRow:String,endRow:String) ={
    //获得T开头的用户
   //val scan=new Scan("T".getBytes,"u".getBytes)
    useTable(x=>{
      val toTable=TableName.valueOf(tableName)
      val flag=tableIsExists(toTable)
      if(flag){
        val table=x.getTable(toTable)
        val scan=new Scan()
        val begin=scan.setStartRow(startRow.getBytes())
        val end=scan.setStopRow(endRow.getBytes())//不包括此行
        val res=table.getScanner(scan)
        if(res.iterator().hasNext)
          getRow(res.next().rawCells())
      }
      else
        logger.warn("table is not exists")
    })
  }
  //过滤器
}
