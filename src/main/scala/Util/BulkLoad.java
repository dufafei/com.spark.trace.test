package Util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class BulkLoad {
    public static class BulkMap extends Mapper<LongWritable,Text,ImmutableBytesWritable,Put> {
        private static final byte[] FAMILY_NAME = "family".getBytes();
        private static final byte[] COLUMN_A = "colA".getBytes();
        private static final byte[] COLUMN_B = "colB".getBytes();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(" ");
            byte[] rowkeybytes = Bytes.toBytes(fields[0]);
            ImmutableBytesWritable rowkey = new ImmutableBytesWritable(rowkeybytes);
            Put put = new Put(rowkeybytes);
            put.addColumn(FAMILY_NAME, COLUMN_A, fields[1].getBytes());
            put.addColumn(FAMILY_NAME, COLUMN_B, fields[2].getBytes());
            context.write(rowkey, put);
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf= HBaseConfiguration.create();
        Job job=Job.getInstance(conf,"load_data_to_" );
        job.setJarByClass(BulkLoad.class);
        job.setMapperClass(BulkMap.class);
        job.setReducerClass(PutSortReducer.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://192.168.132.104:9000/1.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.132.104:9000/output"));
        //生成HFile文件
        Connection conn=ConnectionFactory.createConnection(conf);
        TableName tableName=TableName.valueOf("bulktest");
        Table table=conn.getTable(tableName);
        RegionLocator regionLocator=conn.getRegionLocator(tableName);
        HFileOutputFormat2.configureIncrementalLoad(job,table,regionLocator);
        //导入hbase表
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        loader.doBulkLoad(new Path(""), new HTable(conf,TableName.valueOf("table")));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}

