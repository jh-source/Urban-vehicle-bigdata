package j;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Author: cwz
 * Time: 2017/9/20
 * Description: Hbase创建表，删除表操作
 */
public class HbaseUtils {

    /**
     * 根据表名和列族列表创建表
     *
     * @param tableName 表名
     * @param cols      列族列表
     * @return 是否创建成功
     */
    public static boolean createTable(String tableName, String[] cols) {
        try {
            Connection connection = ConnectionFactory.createConnection(HbaseConf.getConf());
            Admin admin = connection.getAdmin();
            TableName table = TableName.valueOf(tableName);
            if (admin.tableExists(table)) {
                admin.disableTable(table);
                admin.deleteTable(table);
                System.out.println(tableName + " is exists! recreate");
            }
            HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
            admin.close();
            connection.close();
            return true;

        } catch (IOException e) {
            System.out.println("create " + tableName + " failed: " + e.getMessage());
            return false;
        }
    }

    /**
     * 删除指定表
     *
     * @param tableName 表名
     * @return 是否删除成功
     */
    public static boolean deleteTable(String tableName) {
        try {
            Connection connection = ConnectionFactory.createConnection(HbaseConf.getConf());
            Admin admin = connection.getAdmin();
            TableName table = TableName.valueOf(tableName);

            if (admin.tableExists(table)) {
                admin.disableTable(table);
                admin.deleteTable(table);
            }
            admin.close();
            connection.close();
            return true;
        } catch (IOException e) {
            System.out.println("delete " + tableName + " failed: " + e.getMessage());
            return false;
        }
    }
}
