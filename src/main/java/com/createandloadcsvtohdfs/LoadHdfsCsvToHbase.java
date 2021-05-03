package com.createandloadcsvtohdfs;


import com.univocity.parsers.annotations.Convert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.util.StringTokenizer;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class LoadHdfsCsvToHbase {
    public static void main(String[] args) throws Exception{

        //Step1. Read CSV files from Hdfs
        Path pt=new Path("hdfs://localhost:8020/CSVFolder/CSVFiles");
        Configuration conf1 = new Configuration();
        conf1.addResource(new Path("/opt/homebrew/Cellar/hadoop/3.3.0/libexec/etc/hadoop/core-site.xml"));
        conf1.addResource(new Path("/opt/homebrew/Cellar/hadoop/3.3.0/libexec/etc/hadoop/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(conf1);
        FileStatus[] status = fs.listStatus(pt);
        //System.out.println("Print status : "+status);
        LoadToHbase(status,fs);

    }
    public static void LoadToHbase(FileStatus[] status,FileSystem fs) throws Exception
    {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        HTableDescriptor tableName = new HTableDescriptor(TableName.valueOf("peopletable"));
        tableName.addFamily(new HColumnDescriptor("personaldata"));
        tableName.addFamily(new HColumnDescriptor("professionaldata"));
        if (!admin.tableExists(tableName.getTableName())) {
            System.out.print("Creating table. ");
            admin.createTable(tableName);
            System.out.println(" Done.");
        }

        int rowid =0;
        String rowname="";
        //Step1. Create Hbase PeopleTable
        for (int i=0;i<status.length;i++) {
            //System.out.println("---------- Line"+i+" --------");
            //Step2. Load Csv file into Hbase
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
            String line=br.readLine();
            line = br.readLine();
            Table table = connection.getTable(TableName.valueOf("peopletable"));
            while(line!=null && line.length()!=0){
                //System.out.println("Ok till here");
                StringTokenizer tokens = new StringTokenizer(line,",");
                rowname = String.valueOf(++rowid);
                Put p = new Put(Bytes.toBytes((rowname)));
                p.addColumn(Bytes.toBytes("personaldata"),Bytes.toBytes("name"),Bytes.toBytes(tokens.nextToken()));
                p.addColumn(Bytes.toBytes("personaldata"),Bytes.toBytes("age"),Bytes.toBytes(tokens.nextToken()));
                p.addColumn(Bytes.toBytes("personaldata"),Bytes.toBytes("phone_number"),Bytes.toBytes(tokens.nextToken()));
                p.addColumn(Bytes.toBytes("professionaldata"),Bytes.toBytes("company"),Bytes.toBytes(tokens.nextToken()));
                p.addColumn(Bytes.toBytes("personaldata"),Bytes.toBytes("building_code"),Bytes.toBytes(tokens.nextToken()));
                p.addColumn(Bytes.toBytes("personaldata"),Bytes.toBytes("address"),Bytes.toBytes(tokens.nextToken()));
                table.put(p);
                line = br.readLine();
            }
            br.close();
            table.close();
        }
    }
}
