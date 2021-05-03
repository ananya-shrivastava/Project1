package com.createandloadcsvtohdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LoadCsvToHdfs {
    public static void LoadToHdfs(Configuration conf) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(new Path("/Users/ananyashrivastava/IdeaProjects/BigDataProjectQ1-Q2/CSVFiles/"),
                new Path("/CSVFolder"));
    }

}
