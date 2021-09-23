package sycn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wzl
 * @desc
 * @date 2021/9/22 6:42 下午
 **/
public class HBaseToHive extends ToolRunner implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(HBaseToHive.class);

    // HBase
    private static final String HDFS_FS_DEFAULTFS = "fs.defaultFS";
    private static final String HDFS_FS_DEFAULTFS_VALUE = "hdfs://xxx:8020/";
    private static final String HBASE_ROOT_DIR = "hbase.rootdir";
    private static final String HBASE_ROOT_DIR_VALUE = "hdfs://xxxx:8020/hbase";

    // Zookeeper
    private static final String HBASE_NAME = "hbase.zookeeper.quorum";
    private static final String HBASE_VALUE = "xxxx1:2181;xxxx2:2181;xxxx3:2181";

    // AppMaster
    private static final String YARN_APP_MAPREDUCE_AM_STAGING_DIR = "yarn.app.mapreduce.am.staging-dir";

    // IO Serializations
    private static final String HDFS_IO_SERIALIZATIONS = "io.serializations";
    private static final String HDFS_IO_SERIALIZATIONS_VALUE = "org.apache.hadoop.io.serializer.WritableSerialization" +
            ",org.apache.hadoop.io.serializer.avro.AvroSpecificSerialization" +
            ",org.apache.hadoop.io.serializer.avro.AvroReflectSerialization" +
            ",org.apache.hadoop.hbase.mapreduce.MutationSerialization" +
            ",org.apache.hadoop.hbase.mapreduce.ResultSerialization" +
            ",org.apache.hadoop.hbase.mapreduce.KeyValueSerialization";

    //
    private static final String HBASE_CF = "info";
    private static final String STAGE_DIR_PREFIX = "hdfs://xxxx:8020/tmp/hbase/stagedir/";

    @Override
    public int run(String[] strings) throws Exception {
        return 0;
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
