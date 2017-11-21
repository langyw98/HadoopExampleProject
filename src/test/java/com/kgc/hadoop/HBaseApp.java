package com.kgc.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by kgc on 2017/11/21.
 */
public class HBaseApp {

    Connection connection = null;
    Table table = null;
    Admin admin = null;

    @Before
    public void setUp() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("hbase.rootdir","hdfs://192.168.85.128:8020/hbase");
        configuration.set("hbase.zookeeper.quorum", "192.168.85.128:2181");
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
    }

    @Test
    public void template() throws  Exception{

    }

    @Test
    public void delete01() throws  Exception{
        table = connection.getTable(TableName.valueOf("member"));

        Delete delete = new Delete("zhangsan".getBytes());
        table.delete(delete);

    }

    @Test
    public void delete02() throws  Exception{
        table = connection.getTable(TableName.valueOf("member"));

        Delete delete = new Delete("lisi".getBytes());
//        delete.addColumn("info".getBytes(), "birthday".getBytes());

//        delete.addColumns("info".getBytes(), "birthday".getBytes());

        delete.addFamily("info".getBytes());

        table.delete(delete);

    }

    @Test
    public void filter01() throws  Exception{
        table = connection.getTable(TableName.valueOf("member"));

        String req="l^*";
        Scan scan = new Scan();

        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(req));
        scan.setFilter(filter);

        ResultScanner rs = table.getScanner(scan);
        for(Result result : rs){
            printResult(result);
            System.out.println("------------------------------------");
        }
    }

    @Test
    public void filter03() throws  Exception{
        table = connection.getTable(TableName.valueOf("member"));

        String req="l^*";
        Scan scan = new Scan();

        Filter filter1 = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(req));
        Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("z^*"));

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);

        filterList.addFilter(filter1);
        filterList.addFilter(filter2);
        scan.setFilter(filterList);

        ResultScanner rs = table.getScanner(scan);
        for(Result result : rs){
            printResult(result);
            System.out.println("------------------------------------");
        }
    }

    @Test
    public void scan01() throws  Exception{
        table = connection.getTable(TableName.valueOf("member"));
        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        for(Result result : rs){
            printResult(result);
            System.out.println("------------------------------------");
        }
    }

    @Test
    public void scan02() throws  Exception{
        table = connection.getTable(TableName.valueOf("member"));
        Scan scan = new Scan("xiaoqiao".getBytes());
        ResultScanner rs = table.getScanner(scan);
        for(Result result : rs){
            printResult(result);
            System.out.println("------------------------------------");
        }
    }

    @Test
    public void scan03() throws  Exception{
        table = connection.getTable(TableName.valueOf("member"));
        Scan scan = new Scan("wangwu".getBytes(), "xiaoqiao".getBytes());
        ResultScanner rs = table.getScanner(scan);
        for(Result result : rs){
            printResult(result);
            System.out.println("------------------------------------");
        }
    }

    @Test
    public void scan04() throws  Exception{
        table = connection.getTable(TableName.valueOf("member"));
        ResultScanner rs = table.getScanner(Bytes.toBytes("info"));
        for(Result result : rs){
            printResult(result);
            System.out.println("------------------------------------");
        }
    }

    private void printResult(Result result){
        for(Cell cell : result.rawCells()){
            System.out.println(Bytes.toString(result.getRow()) + " " +
                    Bytes.toString(CellUtil.cloneFamily(cell)) + ":" +
                    Bytes.toString(CellUtil.cloneQualifier(cell)) + "=" +
                    Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }
    @Test
    public void get01() throws  Exception{
        table = connection.getTable(TableName.valueOf("member"));

        String rowKey = "lisi";
        Get get = new Get(rowKey.getBytes());
        Result result = table.get(get);
        printResult(result);

    }

    @Test
    public void get02() throws  Exception{
        table = connection.getTable(TableName.valueOf("member"));

        String rowKey = "lisi";
        Get get = new Get(rowKey.getBytes());
        get.addColumn("info".getBytes(),"age".getBytes());
        Result result = table.get(get);

        System.out.println("age:" + Bytes.toString(result.getValue("info".getBytes(),"age".getBytes())));
    }

    @Test
    public void update() throws  Exception{
        table = connection.getTable(TableName.valueOf("member"));

        Put put = new Put(Bytes.toBytes("lisi"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("49"));

        table.put(put);
    }

    @Test
    public void put() throws  Exception{
        table = connection.getTable(TableName.valueOf("member"));

        Put put = new Put(Bytes.toBytes("lisi"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("40"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("birthday"), Bytes.toBytes("1980-09-09"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("company"), Bytes.toBytes("bdqn"));
        put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("country"), Bytes.toBytes("usa"));
        put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("city"), Bytes.toBytes("new york"));

        table.put(put);
    }

    @Test
    public void putBatch() throws  Exception{
        table = connection.getTable(TableName.valueOf("member"));
        List<Put> puts = new ArrayList<Put>();

        Put put1 = new Put(Bytes.toBytes("wangwu"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("40"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("birthday"), Bytes.toBytes("1980-09-09"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("company"), Bytes.toBytes("bdqn"));
        put1.addColumn(Bytes.toBytes("address"), Bytes.toBytes("country"), Bytes.toBytes("usa"));
        put1.addColumn(Bytes.toBytes("address"), Bytes.toBytes("city"), Bytes.toBytes("new york"));

        Put put2 = new Put(Bytes.toBytes("xiaoqiao"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("40"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("birthday"), Bytes.toBytes("1980-09-09"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("company"), Bytes.toBytes("bdqn"));
        put2.addColumn(Bytes.toBytes("address"), Bytes.toBytes("country"), Bytes.toBytes("usa"));
        put2.addColumn(Bytes.toBytes("address"), Bytes.toBytes("city"), Bytes.toBytes("new york"));

        puts.add(put1);
        puts.add(put2);

        table.put(puts);
    }
    @Test
    public void createTable() throws  Exception{
        String tableName = "test_java_api_1";
        String familyName1 = "cf1";
        String familyName2 = "cf2";
        if(admin.tableExists(TableName.valueOf(tableName))){
            System.out.println(tableName + "已存在....");
        }else{
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            tableDescriptor.addFamily(new HColumnDescriptor(familyName1));
            tableDescriptor.addFamily(new HColumnDescriptor(familyName2));
            admin.createTable(tableDescriptor);
            System.out.println("创建成功");
        }
    }



    @Test
    public void queryAllTables() throws  Exception{
        HTableDescriptor[] tableDescriptors = admin.listTables();
        if(tableDescriptors.length > 0){
            for(HTableDescriptor tableDescriptor : tableDescriptors){
                System.out.println(tableDescriptor.getNameAsString());
                for(HColumnDescriptor columnDescriptor : tableDescriptor.getColumnFamilies()){
                    System.out.println("\t" + columnDescriptor.getNameAsString());
                }
            }
        }
    }

    @After
    public void tearDown() throws IOException {
        connection.close();
    }
}
