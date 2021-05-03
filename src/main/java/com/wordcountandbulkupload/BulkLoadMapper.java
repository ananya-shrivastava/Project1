package com.wordcountandbulkupload;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    private static final byte[] CF_BYTES1 = Bytes.toBytes("personaldata");
    private static final byte[] CF_BYTES2 = Bytes.toBytes("professionaldata");
    private static final byte[] QUAL_BYTES1 = Bytes.toBytes("name");
    private static final byte[] QUAL_BYTES2 = Bytes.toBytes("age");
    private static final byte[] QUAL_BYTES3 = Bytes.toBytes("phone_number");
    private static final byte[] QUAL_BYTES4 = Bytes.toBytes("company");
    private static final byte[] QUAL_BYTES5 = Bytes.toBytes("building_code");
    private static final byte[] QUAL_BYTES6 = Bytes.toBytes("address");
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("in map function");
        if (key.get() == 0 || value.getLength() == 0) {
            return;
        }
        String[] strArr = value.toString().split(",");
        byte[] rowKey = Bytes.toBytes(key.get());
        Put put = new Put(rowKey);
        put.addColumn(CF_BYTES1, QUAL_BYTES1, Bytes.toBytes(strArr[0]));
        put.addColumn(CF_BYTES1, QUAL_BYTES2, Bytes.toBytes(strArr[1]));
        put.addColumn(CF_BYTES1, QUAL_BYTES3, Bytes.toBytes(strArr[2]));
        put.addColumn(CF_BYTES2, QUAL_BYTES4, Bytes.toBytes(strArr[3]));
        put.addColumn(CF_BYTES1, QUAL_BYTES5, Bytes.toBytes(strArr[4]));
        put.addColumn(CF_BYTES1, QUAL_BYTES6, Bytes.toBytes(strArr[5]));
        context.write(new ImmutableBytesWritable(rowKey), put);
    }

}


