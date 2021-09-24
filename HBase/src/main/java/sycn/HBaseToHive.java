package sycn;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableMap;
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.UUID;

/**
 * @author wzl
 * @desc HBase to Hive
 * @date 2021/9/22 6:42 下午
 **/
public class HBaseToHive extends ToolRunner implements Tool {
    private static final Logger logger = LoggerFactory.getLogger(HBaseToHive.class);

    /**
     * HBase
     */
    private static final String HDFS_FS_DEFAULTFS = "fs.defaultFS";
    private static final String HDFS_FS_DEFAULTFS_VALUE = "hdfs://xxx:8020/";
    private static final String HBASE_ROOT_DIR = "hbase.rootdir";
    private static final String HBASE_ROOT_DIR_VALUE = "hdfs://xxxx:8020/hbase";

    /**
     * Zookeeper
     */
    private static final String HBASE_NAME = "hbase.zookeeper.quorum";
    private static final String HBASE_VALUE = "xxxx1:2181;xxxx2:2181;xxxx3:2181";

    /**
     * AppMaster
     */
    private static final String YARN_APP_MAPREDUCE_AM_STAGING_DIR = "yarn.app.mapreduce.am.staging-dir";

    /**
     * IO Serializations
     */
    private static final String HDFS_IO_SERIALIZATIONS = "io.serializations";
    private static final String HDFS_IO_SERIALIZATIONS_VALUE = "org.apache.hadoop.io.serializer.WritableSerialization" +
            ",org.apache.hadoop.io.serializer.avro.AvroSpecificSerialization" +
            ",org.apache.hadoop.io.serializer.avro.AvroReflectSerialization" +
            ",org.apache.hadoop.hbase.mapreduce.MutationSerialization" +
            ",org.apache.hadoop.hbase.mapreduce.ResultSerialization" +
            ",org.apache.hadoop.hbase.mapreduce.KeyValueSerialization";

    /**
     * Column Family
     */
    private static final String HBASE_CF = "info";
    /**
     * StageDir
     */
    private static final String STAGE_DIR_PREFIX = "hdfs://xxxx:8020/tmp/hbase/stagedir/";
    /**
     * TmpDir
     */
    private static final String TEP_DIR_PREFIX = "hdfs://xxxx:8020/tmp/hbase/region_tmp/";
    /**
     * Trash Stage Dir
     */
    private static final String STAGE_TRASH_DIR_PREFIX = "hdfs://xxxx:8020/user/hbase_tmp/.Trash/";

    static class TableSnapshotMapper extends MapReduceBase implements TableMap<Text, NullWritable> {
        private Text mkey = new Text();

        @Override
        public void map(ImmutableBytesWritable key, Result result, OutputCollector<Text, NullWritable> outputCollector,
                        Reporter reporter) throws IOException {
            StringBuffer stringBuffer = new StringBuffer();
            CellScanner cellScanner = result.cellScanner();

            SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
            boolean flag = false;
            stringBuffer.append("7");

            // rowkey CellScanner
            while (cellScanner.advance()) {
                Cell cell = cellScanner.current();
                String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                        cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.
                        getValueLength());
                try {
                    if ("task_dt".equals(qualifier) && (format.parse(value).getTime() + 14 * 24 * 60 * 60 *
                            1000) > System.currentTimeMillis()) {
                        flag = true;
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                if ("biz_type".equals(qualifier)) {
                    continue;
                }
                stringBuffer.append("\u0001" + value);
            }
            if (flag && stringBuffer.toString().split("\u0001").length == 21) {
                mkey.set(stringBuffer.toString());
                outputCollector.collect(mkey, NullWritable.get());
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        int result = -1;
        Admin admin = null;
        Connection connection = null;
        String stagingDir = StringUtils.EMPTY;
        String trashStagingDir = StringUtils.EMPTY;
        FileSystem fs = null;
        Configuration hbaseConfiguration = null;
        String snapShotName = StringUtils.EMPTY;
        try {
            // ------------------------------------
            // STEP 1. output_dir
            //
            // ------------------------------------
            // ###
            Configuration config = new Configuration();
            setConf(config);
            // ### output_dir
            Path path = new Path(args[1]);
            fs = path.getFileSystem(config);
            // ###
            if (fs.exists(path)) {
                logger.info("[ScanSnapShotMR] - output_dir is exists");
            }
// ###
            Trash trashTmp = new Trash(fs, config);
           /* if (trashTmp.moveToTrash(path)) {
                LOG.info("Moved to trash: " + path);
            }*/
            // ------------------------------------
            // STEP 2. HbaseSnapShot
            // ------------------------------------
            hbaseConfiguration = HBaseConfiguration.create(config);
            hbaseConfiguration.set(HDFS_IO_SERIALIZATIONS, HDFS_IO_SERIALIZATIONS_VALUE);
            hbaseConfiguration.set(HBASE_ROOT_DIR, HBASE_ROOT_DIR_VALUE);
            String tableName = args[0];
            //
            connection = ConnectionFactory.createConnection(hbaseConfiguration);
            admin = connection.getAdmin();
            if (admin == null) {
                logger.error("[ScanSnapShotMR] - Hbase Admin Error");
                return -1;
            }
//
            boolean exists = admin.tableExists(TableName.valueOf(tableName));
            if (!exists) {
                logger.error("[ScanSnapShotMR] - Hbase tableName = " + tableName + " is not exists.");
                return -1;
            }
// snapshot
            snapShotName = tableName.split(":")[1] + "_snapshot" + UUID.randomUUID().toString().replace("-", "");
//
            logger.info("[ScanSnapShotMR] - snapShotName = " + snapShotName);
            try {
                admin.snapshot(snapShotName, TableName.valueOf(tableName));
            } catch (Exception ex) {
                logger.error("[ScanSnapShotMR] - CloneSnapShot Error", ex);
                return -1;
            }
// ------------------------------------
// STEP 3. HbaseSnapShot
// ------------------------------------
            stagingDir = STAGE_DIR_PREFIX + UUID.randomUUID().toString() + "/";
            trashStagingDir = STAGE_TRASH_DIR_PREFIX + stagingDir;
            hbaseConfiguration.set(YARN_APP_MAPREDUCE_AM_STAGING_DIR, stagingDir);
// ### job
            JobConf jobConf = new JobConf(hbaseConfiguration);
// ### SnapShot
            Path tmpPath = new Path(TEP_DIR_PREFIX + tableName.replaceAll(":", "_") + UUID.randomUUID().
                    toString() + "/");
            Path outPutPath = new Path(args[1]);
            // ### MR
            jobConf.setMapperClass(TableSnapshotMapper.class);
            jobConf.setMapOutputKeyClass(Text.class);
            jobConf.setMapOutputValueClass(NullWritable.class);
            jobConf.setJarByClass(HBaseToHive.class);
            // ------------------------------------
            // STEP 4. MR
            // ------------------------------------
            // ###
            TableMapReduceUtil.initTableSnapshotMapJob(snapShotName,
                    HBASE_CF, TableSnapshotMapper.class, Text.class,
                    NullWritable.class, jobConf, false, tmpPath);
            // ### outputformate
            FileOutputFormat.setOutputPath(jobConf, outPutPath);
            TableMapReduceUtil.addDependencyJars(jobConf);
            // ### job
            RunningJob job = JobClient.runJob(jobConf);
            job.waitForCompletion();
            // ### result
            result = (job.isSuccessful() == true) ? 1 : 0;
        } catch (Exception ex) {
            logger.error("[ScanSnapShotMR] - ex = {}", ex);
            result = -1;
        } finally {
            try {
// ###
                Path path = new Path(stagingDir);
                fs = path.getFileSystem(hbaseConfiguration);
                Trash trashTmp = new Trash(fs, hbaseConfiguration);
                // ###
                if (fs.exists(path)) {
               /* if (trashTmp.moveToTrash(path)) {
                    LOG.info("Moved StagingDir to trash: " + path);
}*/
                }
                // ### snapshot
                logger.info("[ScanSnapShotMR] - SnapShotName = " + snapShotName);
                admin.deleteSnapshot(snapShotName);
                // ###
                admin.close();
                connection.close();
            } catch (Exception ex2) {
                logger.error("[ScanSnapShotMR] - finally ex = {}", ex2);
                result = -1;
            }
        }
// ###
        return result;

    }

    @Override
    public void setConf(Configuration configuration) {
        configuration.set(HBASE_NAME, HBASE_VALUE);
        configuration.set(HDFS_FS_DEFAULTFS, HDFS_FS_DEFAULTFS_VALUE);
    }

    @Override
    public Configuration getConf() {
        return null;
    }

    public static void main(final String[] args) throws Exception {
        if (ArrayUtils.isEmpty(args)) {
            logger.error("[ScanSnapShotMR] - args is not empty!");
            System.exit(-1);
        }

        if (args.length < 2 || args.length > 2) {
            logger.error("[ScanSnapShotMR] - args size must be 2 ; 1) Hbase Table Name 2) Hbase Table Output Dir");
        }

        for (int i = 0; i < args.length; i++) {
            if (logger.isInfoEnabled()) {
                logger.info("[ScanSnapShotMR] - arg(" + i + ")=" + args[i]);
            }
        }

        ToolRunner.run(HBaseConfiguration.create(), new HBaseToHive(), args);
    }
}
