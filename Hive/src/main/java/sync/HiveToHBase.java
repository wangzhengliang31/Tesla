package sync;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MapReduceExtendedCell;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;

/**
 * @author wzl
 * @desc
 * @date 2021/9/24 5:21 下午
 **/
public class HiveToHBase {
    // ### ZK
    public static final String ZK = "xxx1:2181,xxx2:2181,xxx3:2181";
    // ###
    public static final String SPLIT_SYMBOL = "\001";
    // ###
    public static final String CF = "CF";
    // ###
    public static final Log LOG = LogFactory.getLog(HiveToHBase.class);
    // ### Hive NULL
    public static final String HIVE_NULL_CONSTANT = "\\N";
    // ###
    public static final String EXEC_USER = "hbase";

    // ### MapClass
    public static class MapClass extends Mapper<LongWritable, Text, ImmutableBytesWritable, Cell> {
        // [0] - RowKey
        private String rowKey;
        // [1] - id
        private String id;
        // [2] -
        private String assistantNumber;
        // [3] -
        // 2020-06-25 19:00:41
        private String createTime;
        // [4] - userId
        private String recordType;
        // [5] -
        private String content;
        // [6] - ID
        private String targetUserId;
        // [7] -
        private String subclazzNumber;
        // [8] -
        private String assistantName;
        // Hbase
        private ImmutableBytesWritable outKey;
        private Cell cell;
        // phoenixLong
        PLong phoenixLong;
        PInteger pInteger;
        // MD5
        MessageDigest md;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            try {
                md = MessageDigest.getInstance("MD5");
                phoenixLong = PLong.INSTANCE;
                pInteger = PInteger.INSTANCE;
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

        private String md5RowKey(String rowKey) {
            md.update(rowKey.getBytes());
            byte b[] = md.digest();
            int i;
            StringBuffer buf = new StringBuffer("");
            for (int offset = 0; offset < b.length; offset++) {
                i = b[offset];
                if (i < 0) {
                    i += 256;
                }
                if (i < 16) {
                    buf.append("0");
                }
                buf.append(Integer.toHexString(i));
            }
            return buf.toString().substring(8, 24);
        }

        @Override
        public void map(LongWritable inKey, Text inValue, Context context) throws ClassCastException {
            // ###
            String[] infos = inValue.toString().split(SPLIT_SYMBOL);
            try {
                String msg = String.format("id=%s,rowkey=%s,assistantNumber=%s,createTime=%s,recordType=%s, content=%s,targetUserId=%s,subclazzNumber=%s,assistantName=%s",
                        id, rowKey, assistantNumber, createTime, recordType, content, targetUserId, subclazzNumber,
                        assistantName);
                //
                if (LOG.isInfoEnabled()) {
                    LOG.info(msg);
                }
                // ###
                if (!StringUtils.isBlank(id) && !StringUtils.equals(id, HIVE_NULL_CONSTANT)) {
                    // rowkey
                    rowKey = md5RowKey(id);
                    // ### rowkey
                    outKey = new ImmutableBytesWritable(Bytes.toBytes(rowKey));
                    cell = new KeyValue(Bytes.toBytes(rowKey), Bytes.toBytes(CF), Bytes.toBytes("ID"),
                            phoenixLong.toBytes(Long.parseLong(id)));
                    context.write(outKey, new MapReduceExtendedCell(cell));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // ### Main
    public static void main(String[] args) throws Exception {
        // ###
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", ZK);
        final String[] paramArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job hfileJob = Job.getInstance(conf);
        hfileJob.setJobName("HbaseBulkLoadDemoMR");
        hfileJob.setJarByClass(HiveToHBase.class);
        hfileJob.setMapperClass(MapClass.class);
        hfileJob.setNumReduceTasks(0);
        hfileJob.setOutputKeyClass(ImmutableBytesWritable.class);
        hfileJob.setOutputKeyClass(HiveToHBase.class);


        FileInputFormat.addInputPath(hfileJob, new Path(paramArgs[0]));
        FileOutputFormat.setOutputPath(hfileJob, new Path(paramArgs[1]));
// --------------------------------------------------
// Hbase
// --------------------------------------------------
        final Configuration hbaseConfiguration = HBaseConfiguration.create(conf);
        hbaseConfiguration.set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization,org.apache.hadoop.io.serializer.avro.AvroSpecificSerialization, org.apache.hadoop.io.serializer.avro. AvroReflectSerialization, org.apache.hadoop.hbase.mapreduce.MutationSerialization, org.apache.hadoop.hbase. mapreduce.ResultSerialization, org.apache.hadoop.hbase.mapreduce.KeyValueSerialization");
        final Connection connection = ConnectionFactory.createConnection(hbaseConfiguration);
        final TableName tableName = TableName.valueOf(paramArgs[2]);
        final Table clazzLessonTable = connection.getTable(tableName);
        HFileOutputFormat2.configureIncrementalLoad(hfileJob, clazzLessonTable, connection.getRegionLocator
                (tableName));
        int result = hfileJob.waitForCompletion(true) ? 0 : 1;
        // ###
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(EXEC_USER);
        try {
            ugi.doAs(new PrivilegedExceptionAction<String>() {
                @Override
                public String run() throws Exception {
                    Configuration conf2 = new Configuration();
                    conf2.set("hbase.zookeeper.quorum", ZK);
                    conf2.set("hadoop.security.authentication", "simple");
                    Configuration hbaseConfiguration2 = HBaseConfiguration.create(conf2);
                    hbaseConfiguration2.set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization, org.apache.hadoop.io.serializer.avro.AvroSpecificSerialization, org.apache.hadoop.io. serializer.avro.AvroReflectSerialization, org.apache.hadoop.hbase.mapreduce.MutationSerialization, org.apache. hadoop.hbase.mapreduce.ResultSerialization, org.apache.hadoop.hbase.mapreduce.KeyValueSerialization");
                    Connection connection2 = ConnectionFactory.createConnection(hbaseConfiguration2);
                    Admin admin2 = connection2.getAdmin();
                    TableName tableName2 = TableName.valueOf(paramArgs[2]);
                    Table clazzLessonTable2 = connection.getTable(tableName);
                    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbaseConfiguration2);
                    loader.doBulkLoad(new Path(paramArgs[1]), admin2, clazzLessonTable2, connection2.
                            getRegionLocator(tableName2));
                    return "null";
                }
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        }
// ###
        System.exit(result);
    }
}
