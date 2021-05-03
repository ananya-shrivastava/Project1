package com.wordcountandbulkupload;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BulkLoadDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new BulkLoadDriver(), args);
        System.exit(status);
    }

    private static Job createSubmittableJob(Configuration conf, String tableNameString, Path tmpPath, String input)
            throws IOException {

        System.out.println("conf: " + conf);
        Job job = Job.getInstance(conf, "HBase Bulk Load Example");
        job.setJarByClass(BulkLoadMapper.class); //check
        job.setMapperClass(BulkLoadMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        FileOutputFormat.setOutputPath(job, tmpPath);
        FileInputFormat.addInputPath(job, new Path(input));
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            TableName tableName = TableName.valueOf(tableNameString);
            Table table = connection.getTable(tableName);
            RegionLocator regionLocator = connection.getRegionLocator(tableName);
            HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
        }
        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create(getConf());
        setConf(conf);

        String tableNameString = "peopletable";
        Path tmpPath = new Path("hdfs://localhost:8020/BulkUploadHbase");
        String input = "hdfs://localhost:8020/CSVFolder/CSVFiles";
        Job job = createSubmittableJob(conf, tableNameString, tmpPath, input);
        boolean success = job.waitForCompletion(true);
        doBulkLoad(tableNameString, tmpPath);
        return success ? 0 : 1;
    }

    private void doBulkLoad(String tableNameString, Path tmpPath) throws Exception {
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(getConf());
        try (Connection connection = ConnectionFactory.createConnection(getConf()); Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(tableNameString);
            Table table = connection.getTable(tableName);
            RegionLocator regionLocator = connection.getRegionLocator(tableName);
            loader.doBulkLoad(tmpPath, admin, table, regionLocator);
        }
    }
}
