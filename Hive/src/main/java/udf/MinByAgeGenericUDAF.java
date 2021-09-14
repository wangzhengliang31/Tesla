package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wzl
 * @desc UDAF AbstractGenericUDAFResolver 检查参数，GenericUDAFEvaluator 具体实现
 * @date 2021/9/9 4:48 下午
 **/
@Description(name = "min_by_age", value = "_FUNC(name, age) - Return the youngest user name")
public class MinByAgeGenericUDAF extends AbstractGenericUDAFResolver {

    private static final Logger logger = LoggerFactory.getLogger(AbstractGenericUDAFResolver.class);

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        // 参数个数大于1报错
        if (parameters.length != 2) {
            throw new UDFArgumentLengthException("Exactly two arguments are expected: " + parameters.length);
        }
        // 参数类型是否支持比较
        ObjectInspector yOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[1]);
        if (!ObjectInspectorUtils.compareSupported(yOI)) {
            throw new UDFArgumentTypeException(1,
                    "Cannot support comparison of map<> type or complex type containing map<>.");
        }
        return new Evaluator();
    }

    public static class Evaluator extends GenericUDAFEvaluator {

        private transient ObjectInspector xInputOI, yInputOI;
        private transient ObjectInspector xOutputOI, yOutputOI;
        @Nullable
        private transient StructField xField, yField;
        @Nullable
        private transient StructObjectInspector partialInputOI;


        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                this.xInputOI = parameters[0];
                this.yOutputOI = parameters[1];
                if (!ObjectInspectorUtils.compareSupported(yInputOI)) {
                    throw new UDFArgumentTypeException(1,
                            "Cannot support comparison of map<> type or complex type containing map<>.");
                }
            } else {
                this.partialInputOI = (StructObjectInspector) parameters[0];
                this.xField = partialInputOI.getStructFieldRef("name");
                this.xInputOI = xField.getFieldObjectInspector();
                this.yField = partialInputOI.getStructFieldRef("age");
                this.yInputOI = yField.getFieldObjectInspector();
            }
            this.xOutputOI = ObjectInspectorUtils.getStandardObjectInspector(xInputOI,
                    ObjectInspectorCopyOption.JAVA);
            this.yOutputOI = ObjectInspectorUtils.getStandardObjectInspector(yInputOI,
                    ObjectInspectorCopyOption.JAVA);

            final ObjectInspector outputOI;
            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {// terminatePartial
                List<String> fieldNames = new ArrayList<>(2);
                List<ObjectInspector> fieldOIs = new ArrayList<>(2);
                fieldNames.add("x");
                fieldOIs.add(xOutputOI);
                fieldNames.add("y");
                fieldOIs.add(yOutputOI);
                return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
            } else {// terminate
                // Copy to Java object because that saves object creation time.
                outputOI = ObjectInspectorUtils.getStandardObjectInspector(xInputOI,
                        ObjectInspectorCopyOption.JAVA);
            }
            return outputOI;
        }

        /** class for storing the current max value */
        @AggregationType(estimable = false)
        static class MinAggregation extends AbstractAggregationBuffer {
            Object x, y;

            @Override
            public int estimate() {
                return JavaDataModel.PRIMITIVES2 * 2; // rough estimate
            }

            void merge(final Object newX, final Object newY,
                       @Nonnull final ObjectInspector xInputOI,
                       @Nonnull final ObjectInspector yInputOI,
                       @Nonnull final ObjectInspector yOutputOI) {
                final int cmp = ObjectInspectorUtils.compare(y, yOutputOI, newY, yInputOI);
                if (x == null || (newX != null && cmp > 0)) {// found smaller y
                    this.x = ObjectInspectorUtils.copyToStandardObject(newX, xInputOI,
                            ObjectInspectorCopyOption.JAVA);
                    this.y = ObjectInspectorUtils.copyToStandardObject(newY, yInputOI,
                            ObjectInspectorCopyOption.JAVA);
                }
            }
        }


        // 创建新的聚合计算需要的内存以用来计算
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new MinAggregation();
        }

        // 内存重用
        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            MinAggregation myagg = (MinAggregation) aggregationBuffer;
            myagg.x = null;
            myagg.y = null;
        }

        // map阶段调用
        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] parameters) throws HiveException {
            assert (parameters.length == 2);
            MinAggregation myagg = (MinAggregation) aggregationBuffer;
            Object x = parameters[0];
            Object y = parameters[1];

            myagg.merge(x, y, xInputOI, yInputOI, yOutputOI);

        }

        // mapper、combiner结束要返回的结果
        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            MinAggregation myagg = (MinAggregation) aggregationBuffer;
            Object[] partial = new Object[2];
            partial[0] = myagg.x;
            partial[1] = myagg.y;
            return partial;
        }

        // combiner合并map返回的结果，或reducer合并mapper或combiner返回的结果
        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }

            MinAggregation myagg = (MinAggregation) aggregationBuffer;
            Object x = partialInputOI.getStructFieldData(partial, xField);
            Object y = partialInputOI.getStructFieldData(partial, yField);

            myagg.merge(x, y, xInputOI, yInputOI, yOutputOI);

        }

        // reducer返回的结果，或者没有reduce时mapper端返回的结果
        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            MinAggregation myagg = (MinAggregation) aggregationBuffer;
            return myagg.x;
        }
    }


}
