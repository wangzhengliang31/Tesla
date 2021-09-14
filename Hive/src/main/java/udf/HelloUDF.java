package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author wzl
 * @desc 打印 "Hello str" UDF
 * @date 2021/9/14 10:16 上午
 **/
@Description(
        name = "Hello",
        value = "_FUNC_(str) - return the value that is \"Hello str\"",
        extended = "Example:\nSELECT _FUNC_(str) FROM src;"
)
public class HelloUDF extends UDF {
    public String evalute(String str) {
        return "Hello " + str;
    }
}
