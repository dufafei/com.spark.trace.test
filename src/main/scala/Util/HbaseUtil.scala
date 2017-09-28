package Util

import Init.Pattern
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.log4j.{Level, Logger}
/**
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
  def createTable(tableName:String,familyName:Array[String]): Unit ={
   useAdmin(x=>{
     val table=TableName.valueOf(tableName)
     if(x.tableExists(table))
       logger.warn(s"create table:$tableName is already exists")
     else {
       val tableDesc=new HTableDescriptor(table)
       familyName.indices.map(x => {
         val family=new HColumnDescriptor(familyName(x))
         tableDesc.addFamily(family)
       })
       x.createTable(tableDesc)
     }
   })
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
  /**
    *
    * @param tableName
    * @param rowKey
    * @param familyName
    * @param columnName
    * @param value
    */
  def insertLine(tableName:String,rowKey:String,familyName:String,columnName:String,value:String): Unit ={
    useTable(x=>{
      val toTable=TableName.valueOf(tableName)
      val flag=tableIsExists(toTable)
      if(flag){
        val table=x.getTable(toTable)
        val put=new Put(rowKey.getBytes(this.hbaseEncode))
        put.addColumn(familyName.getBytes,columnName.getBytes(),value.getBytes())
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
  /**
    *
    * @param tableName
    * @param rowkeys
    */
  def deleteLine(tableName:String,rowkeys:Array[String]): Unit ={
    useTable(x=> {
      val toTable=TableName.valueOf(tableName)
      val flag=tableIsExists(toTable)
      if (flag){
        val table=x.getTable(toTable)
        val deleteList=rowkeys.map(x=>{
          new Delete(x.getBytes(this.hbaseEncode))
        }).toList
        import scala.collection.JavaConversions._
        table.delete(deleteList)
      }
     else
        logger.warn("table is not exists")
    })
  }
  //获取行-get
  /**
    *
    * @param tableName
    * @param rowkey
    * @param familyName
    * @param columnName
    * @return
    */
  def getLine(tableName:String,rowkey:String,familyName:String,columnName:String) ={
    useTable(x=>{
      val toTable=TableName.valueOf(tableName)
      val flag=tableIsExists(toTable)
      if (flag){
        val table=x.getTable(toTable)
        val get=new Get(rowkey.getBytes(this.hbaseEncode))
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
  /**
    *
    * @param tableName
    * @param startRow
    * @param endRow
    * @return
    */
  def getLines(tableName:String,startRow:String,endRow:String) ={
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
}
