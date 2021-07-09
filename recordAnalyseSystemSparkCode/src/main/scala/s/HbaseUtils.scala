package s

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}

/**
  * Author: cwz
  * Time: 2017/9/19
  * Description: Hbase创建表，删除表操作
  */
object HbaseUtils {
  /**
    * 根据表名和列族列表创建表
    *
    * @param tableName 表名
    * @param cols      列族列表
    * @return 是否创建成功
    */
  def createTable(tableName: String, cols: Array[String]): Boolean = {
    try {
      val connection = ConnectionFactory.createConnection(HbaseConf.getConf())
      val admin = connection.getAdmin()
      val table = TableName.valueOf(tableName)
      if (admin.tableExists(table)) {
        admin.disableTable(table)
        admin.deleteTable(table)
        println(s"${tableName} is exists! recreate")
      }
      val hTableDescriptor = new HTableDescriptor(table)
      for (col <- cols) {
        val hColumnDescriptor = new HColumnDescriptor(col)
        hTableDescriptor.addFamily(hColumnDescriptor)
      }
      admin.createTable(hTableDescriptor)
      admin.close()
      connection.close()
      true

    } catch {
      case e: Exception => {
        println(s"create ${tableName} failed: ${e.getMessage}")
        false
      }
    }
  }

  /**
    * 删除指定表
    *
    * @param tableName 表名
    * @return 是否删除成功
    */
  def deleteTable(tableName: String): Boolean = {
    try {
      val connection = ConnectionFactory.createConnection(HbaseConf.getConf())
      val admin = connection.getAdmin()
      val table = TableName.valueOf(tableName)

      if (admin.tableExists(table)) {
        admin.disableTable(table)
        admin.deleteTable(table)
      }
      admin.close()
      connection.close()
      true
    } catch {
      case e: Exception => {
        println(s"delete ${tableName} failed: ${e.getMessage}")
        false
      }
    }
  }
}
