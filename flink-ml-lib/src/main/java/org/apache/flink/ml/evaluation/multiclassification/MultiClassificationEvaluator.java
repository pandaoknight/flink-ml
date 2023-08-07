package org.apache.flink.ml.evaluation.multiclassification;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.ml.api.AlgoOperator;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MultiClassificationEvaluator
        implements AlgoOperator<MultiClassificationEvaluator>,
        MultiClassificationEvaluatorParams<MultiClassificationEvaluator> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public MultiClassificationEvaluator() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    public Table[] transform(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        StreamTableEnvironment tEnv = (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();

        DataStream<Row> input = tEnv.toDataStream(inputs[0]);
        SingleOutputStreamOperator<Row> t = input.filter((FilterFunction<Row>) a -> a.getField(0).equals(a.getField(1)));
        SingleOutputStreamOperator<Row> f = input.filter((FilterFunction<Row>) a -> !a.getField(0).equals(a.getField(1)));
        SingleOutputStreamOperator<Metrics> ds_tp = t.map((MapFunction<Row, Metrics>) a -> new Metrics((String) a.getField(0), 1, 0, 0, 0));
        SingleOutputStreamOperator<Metrics> ds_fp = f.map((MapFunction<Row, Metrics>) a -> new Metrics((String)a.getField(1), 0, 1, 0, 0));
        SingleOutputStreamOperator<Metrics> ds_fn = f.map((MapFunction<Row, Metrics>) a -> new Metrics((String)a.getField(0), 0, 0, 1, 0));
//        tp.print();
//        fp.print();
        DataStream<Metrics> c = ds_tp.union(ds_fp, ds_fn);
//        c.print();
        KeyedStream<Metrics, String> ck = c.keyBy(a -> a.label);
//        ck.print();
        SingleOutputStreamOperator<Metrics> ckr = ck.reduce((a, b) -> new Metrics(a.label, a.tp + b.tp, a.fp + b.fp, a.fn + b.fn, a.tn + b.tn));
//        ckr.print(); //1)条数并没有真正变少，这个是最大的问题，这样会给后面的collect()造成更大的负担。

        Iterator<Metrics> resultIterator = org.apache.flink.streaming.api.datastream.DataStreamUtils.collect(ckr);
        ConcurrentHashMap<String, Metrics> metricsMap = new ConcurrentHashMap<>(); //*本地对象)
        while (resultIterator.hasNext()) {
            Metrics value = resultIterator.next();
            metricsMap.put(value.label, value);
        }
        System.out.println(metricsMap);
        //
//        double totalPrecision = 0.0;
//        for (Map.Entry<String, Metrics> entry : metricsMap.entrySet()) {
//            Metrics metrics = entry.getValue();
//            double precision = (double) metrics.tp / (metrics.tp + metrics.fp);
//            totalPrecision += precision;
//        }
//
//        double macroPrecision = totalPrecision / metricsMap.size();
//
//        System.out.println("Macro-Precision: " + macroPrecision);

        DataStream<Integer> count = DataStreamUtils.reduce(input.map((MapFunction<Row, Integer>) (a -> 1)), Integer::sum);
        count.print();
        Iterator<Integer> iterator = org.apache.flink.streaming.api.datastream.DataStreamUtils.collect(count);

        double all = iterator.next();
        System.out.println(all);

        double totalTp = 0.0;
        double totalFp = 0.0;
        double totalFn = 0.0;
        double totalTn = 0.0;

        double totalPrecision = 0.0;
        double totalRecall = 0.0;
        double totalF1Score = 0.0;
        double totalAccuracy = 0.0;

        for (Map.Entry<String, Metrics> entry : metricsMap.entrySet()) {
            Metrics metrics = entry.getValue();
            double tp = metrics.tp;
            double fp = metrics.fp;
            double fn = metrics.fn;
            double tn = all - tp - fp - fn;

            totalTp += tp;
            totalFp += fp;
            totalFn += fn;
            totalTn += tn;

            double precision = tp / (tp + fp);
            double recall = tp / (tp + fn);
            double f1Score = 2 * precision * recall / (precision + recall);
            double accuracy = (tp + tn) / (tp + fp + fn + tn);

            totalPrecision += precision;
            totalRecall += recall;
            totalF1Score += f1Score;
            totalAccuracy += accuracy;
        }

        int size = metricsMap.size();

        double macroPrecision = totalPrecision / size;
        double macroRecall = totalRecall / size;
        double macroF1Score = totalF1Score / size;
        double macroAccuracy = totalAccuracy / size;

        double microPrecision = totalTp / (totalTp + totalFp);
        double microRecall = totalTp / (totalTp + totalFn);
        double microF1Score = 2 * microPrecision * microRecall / (microPrecision + microRecall);
        double microAccuracy = (totalTp + totalTn) / (totalTp + totalFp + totalFn + totalTn);

//        Schema tableSchema = Schema.newBuilder()
//                .column("macroAccuracy", DataTypes.DOUBLE())
//                .column("macroPrecision", DataTypes.DOUBLE())
//                .column("macroRecall", DataTypes.DOUBLE())
//                .column("macroF1Score", DataTypes.DOUBLE())
//                .column("microAccuracy", DataTypes.DOUBLE())
//                .column("microPrecision", DataTypes.DOUBLE())
//                .column("microRecall", DataTypes.DOUBLE())
//                .column("microF1Score", DataTypes.DOUBLE())
//                .build();
//
//        Table resultsTable = tEnv.fromValues(
//                Expressions.row(
//                        macroAccuracy,
//                        macroPrecision,
//                        macroRecall,
//                        macroF1Score,
//                        microAccuracy,
//                        microPrecision,
//                        microRecall,
//                        microF1Score
//                ),
//                tableSchema
//        );
//        tEnv.toDataStream(resultsTable).print(); //?

        DataType dataType = DataTypes.ROW(
                DataTypes.FIELD("macroAccuracy", DataTypes.DOUBLE()),
                DataTypes.FIELD("macroPrecision", DataTypes.DOUBLE()),
                DataTypes.FIELD("macroRecall", DataTypes.DOUBLE()),
                DataTypes.FIELD("macroF1Score", DataTypes.DOUBLE()),
                DataTypes.FIELD("microAccuracy", DataTypes.DOUBLE()),
                DataTypes.FIELD("microPrecision", DataTypes.DOUBLE()),
                DataTypes.FIELD("microRecall", DataTypes.DOUBLE()),
                DataTypes.FIELD("microF1Score", DataTypes.DOUBLE())
        );

        // Create some data
        Row row = Row.of(
                macroAccuracy,
                macroPrecision,
                macroRecall,
                macroF1Score,
                microAccuracy,
                microPrecision,
                microRecall,
                microF1Score);

        // Create a table from the data
        Table resultsTable = tEnv.fromValues(dataType, Arrays.asList(row));
        tEnv.toDataStream(resultsTable).print(); //?

        return new Table[] { resultsTable };

        //        KeyedStream<Metrics, String> tpk = tp
//                .returns(TypeInformation.of(new TypeHint<Metrics>() {}))
//                .keyBy(a -> a.label);
//        KeyedStream<Metrics, String> fpk = fp
//                .returns(TypeInformation.of(new TypeHint<Metrics>() {}))
//                .keyBy(a -> a.label);
//        KeyedStream<Metrics, String> fnk = fn
//                .returns(TypeInformation.of(new TypeHint<Metrics>() {}))
//                .keyBy(a -> a.label);
//        tpk.print();
//        fnk.print();
    }

    public static class Metrics { //
        public String label;
        public long tp;       // The number of true positives.
        public long fp;       // The number of false positives.
        public long fn;       // The number of false negatives.
        public long tn;       // The number of true negatives.

        public Metrics(String label, long tp, long fp, long fn, long tn) {
            this.label = label;
            this.tp = tp;
            this.fp = fp;
            this.fn = fn;
            this.tn = tn;
        }

        @Override
        public String toString() {
            return "label:" + this.label + " tp:" + this.tp + " fp:" + this.fp + " fn:" + this.fn + " tn:" + this.tn;
        }
    }
    public Table[] transform_01(Table... inputs) {

        Preconditions.checkArgument(inputs.length == 1);
        StreamTableEnvironment tEnv = (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();

        DataStream<Row> input = tEnv.toDataStream(inputs[0]);
//        input.print();
        SingleOutputStreamOperator<Row> t = input.filter((FilterFunction<Row>) a -> a.getField(0).equals(a.getField(1)));
//        t.print();
        SingleOutputStreamOperator<Row> f = input.filter((FilterFunction<Row>) a -> !a.getField(0).equals(a.getField(1)));
//        SingleOutputStreamOperator<Integer> tc = t.map((MapFunction<Row, Integer>) (a -> 1));
//        tc.print();
//        SingleOutputStreamOperator<Tuple2<String, Integer>> ttc = t.map(
//                (MapFunction<Row, Tuple2<String, Integer>>) (
//                        a -> new Tuple2<>((String) a.getField(0), 1)
//                ));
//        ttc.print();
//        KeyedStream<Row, String> fp = f.keyBy(a -> (String)a.getField(0));
//        KeyedStream<Row, String> fn = f.keyBy(a -> (String)a.getField(1));
        KeyedStream<Tuple2<String, Integer>, String> tp = t
                .map((MapFunction<Row, Tuple2<String, Integer>>) a -> new Tuple2<>((String)a.getField(0), 1))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
                .keyBy(a -> a.f0);
//        tp.sum(1).print(); //tp.sum)和tp.reduce可以等效
//        tp.print();
        SingleOutputStreamOperator<Tuple2<String, Integer>> tpr = tp.reduce((ReduceFunction<Tuple2<String, Integer>>) (a, b) -> new Tuple2<>(a.f0, a.f1 + b.f1));
//        tpr.print();


//        fp.print();
//        fn.print();
//        SingleOutputStreamOperator<Integer> tpc = tp.map((MapFunction<Row, Integer>) (a -> 1));
//        tpc.print();
//        DataStream<Integer> tpcnt = DataStreamUtils.reduce(tp.map((MapFunction<Row, Integer>) (a -> 1)), Integer::sum);
//        tpcnt.print();
//        DataStream<Integer> tpcnt = DataStreamUtils.reduce(tp.map((MapFunction<Row, Integer>) (a -> 1)), Integer::sum);
//        DataStream<Integer> tpcnt = DataStreamUtils.reduce(tp.map((MapFunction<Row, Integer>) (a -> 1)), Integer::sum);
//        DataStream<Integer> tpcnt = DataStreamUtils.reduce(tp.map((MapFunction<Row, Integer>) (a -> 1)), Integer::sum);
        return new Table[] { tEnv.fromDataStream(tp) };
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static MultiClassificationEvaluator load(StreamTableEnvironment env, String path)
            throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

}