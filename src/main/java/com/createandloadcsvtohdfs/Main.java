package com.createandloadcsvtohdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import static com.createandloadcsvtohdfs.CreateCsv.createPeopleCsv;
import static com.createandloadcsvtohdfs.LoadCsvToHdfs.LoadToHdfs;

public class Main {
    public static void main(String[] args) throws Exception{
        //Create CSV files
        createPeopleCsv(10);

        Configuration conf = new Configuration();
        conf.addResource(new Path("/opt/homebrew/Cellar/hadoop/3.3.0/libexec/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/opt/homebrew/Cellar/hadoop/3.3.0/libexec/etc/hadoop/hdfs-site.xml"));

        //Load all csv files to hdfs
        LoadToHdfs(conf);
    }
}
