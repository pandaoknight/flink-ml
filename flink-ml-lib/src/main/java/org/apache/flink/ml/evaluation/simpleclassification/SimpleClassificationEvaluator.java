package org.apache.flink.ml.evaluation.simpleclassification;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.AlgoOperator;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class SimpleClassificationEvaluator
        implements AlgoOperator<SimpleClassificationEvaluator>,
                    SimpleClassificationEvaluatorParams<SimpleClassificationEvaluator> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();
    //private static final Logger LOG = LoggerFactory.getLogger(SimpleClassificationEvaluator.class);

    public SimpleClassificationEvaluator() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public Table[] transform(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        StreamTableEnvironment tEnv = (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        DataStream<Row> inputData = tEnv.toDataStream(inputs[0]);
        //m1)
        DataStream<Metrics> metricsData = inputData.map(new MetricsMapper());
//        metricsData.print();
        //r2)
        DataStream<Metrics> reducedMetricsData = DataStreamUtils.reduce(metricsData, new MetricsReducer());
        //t1)变换为需要的指标。
        final String[] metricsNames = getMetricsNames();
        TypeInformation<?>[] metricTypes = new TypeInformation[metricsNames.length];
        Arrays.fill(metricTypes, Types.DOUBLE);
        RowTypeInfo outputTypeInfo = new RowTypeInfo(metricTypes, metricsNames);
        DataStream<Row> evalResult =
            reducedMetricsData.map(
                (MapFunction<Metrics, Row>)
                    value -> {
                        double tp = value.tp;
                        double fp = value.fp;
                        double tn = value.tn;
                        double fn = value.fn;

                        double accuracy = (tp + tn) / (tp + fp + tn + fn);
                        double precision = tp / (tp + fp);
                        double recall = tp / (tp + fn);
                        double f1Score = 2 * (precision * recall) / (precision + recall);

                        Row ret = new Row(4);
                        ret.setField(0, accuracy);
                        ret.setField(1, precision);
                        ret.setField(2, recall);
                        ret.setField(3, f1Score);

                        return ret;
                    },
                    outputTypeInfo
                    //new RowTypeInfo(Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, getMetricsNames())
                );
//        evalResult.print();
        return new Table[] { tEnv.fromDataStream(evalResult) };
    }

    public static class Metrics { //
//        public String label;
        public long tp;       // The number of true positives.
        public long fp;       // The number of false positives.
        public long tn;       // The number of true negatives.
        public long fn;       // The number of false negatives.

        public Metrics(long tp, long fp, long tn, long fn) {
            this.tp = tp;
            this.fp = fp;
            this.tn = tn;
            this.fn = fn;
        }

        @Override
        public String toString() {
            return "tp:" + this.tp + " fp:" + this.fp + " tn:" + this.tn + " fn:" + this.fn;
        }
    }

    public static class MetricsMapper implements MapFunction<Row, Metrics> {
        @Override
        public Metrics map(Row value) throws Exception {
            double label = (double) value.getField(0);  // The label.
            double predict = (double) value.getField(1);  // The prediction.
            long tp = 0, fp = 0, tn = 0, fn = 0;
            tp += (label == 1.0 && predict == 1.0) ? 1 : 0;
            tn += (label == 0.0 && predict == 0.0) ? 1 : 0;
            fp += (label == 0.0 && predict == 1.0) ? 1 : 0;
            fn += (label == 1.0 && predict == 0.0) ? 1 : 0;
            return new Metrics(tp, fp, tn, fn);
        }
    }

    public static class MetricsReducer implements ReduceFunction<Metrics> {
        @Override
        public Metrics reduce(Metrics value1, Metrics value2) throws Exception {
            return new Metrics(
                    value1.tp + value2.tp,
                    value1.fp + value2.fp,
                    value1.tn + value2.tn,
                    value1.fn + value2.fn
            );
        }
    }

    public static class MetricsAggregate implements AggregateFunction<Metrics, Metrics, Metrics> {

        @Override
        public Metrics createAccumulator() {
            return null;
        }

        @Override
        public Metrics add(Metrics metrics, Metrics metrics2) {
            return null;
        }

        @Override
        public Metrics getResult(Metrics metrics) {
            return null;
        }

        @Override
        public Metrics merge(Metrics metrics, Metrics acc1) {
            return null;
        }
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static SimpleClassificationEvaluator load(StreamTableEnvironment env, String path)
            throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }
}
