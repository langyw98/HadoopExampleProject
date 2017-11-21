package com.kgc.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;

/**
 * Created by kgc on 2017/11/21.
 */
public class CopyHbaseTableApp {

    public static class MyMapper extends TableMapper<Text, Put>{
        Text mapOutputKey = new Text();

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            mapOutputKey.set(Bytes.toString(key.get()));

            Put put = new Put(key.get());
            for(Cell cell : value.rawCells()){
                if("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
                    if("age".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                        put.add(cell);
                    }
                    if("company".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                        put.add(cell);
                    }
                }
            }
            context.write(mapOutputKey, put);
        }
    }

    public static class MyReduce extends TableReducer<Text, Put, ImmutableBytesWritable>{
        @Override
        protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
            for(Put put: values){
                context.write(null, put);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        Job job = new Job(config,"CopyHbaseTableApp");
        job.setJarByClass(CopyHbaseTableApp.class);    // class that contains mapper

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
// set other scan attrs

        TableMapReduceUtil.initTableMapperJob(
                args[0],      // input table
                scan,             // Scan instance to control CF and attribute selection
                MyMapper.class,   // mapper class
                Text.class,             // mapper output key
                Put.class,             // mapper output value
                job);
        TableMapReduceUtil.initTableReducerJob(
                args[1],      // output table
                MyReduce.class,             // reducer class
                job);
        job.setNumReduceTasks(0);

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }
}
