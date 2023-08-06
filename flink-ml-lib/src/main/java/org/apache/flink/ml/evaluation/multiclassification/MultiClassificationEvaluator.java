package org.apache.flink.ml.evaluation.multiclassification;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.AlgoOperator;
import org.apache.flink.ml.common.datastream.DataStreamUtils;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
//        tp.print();
        SingleOutputStreamOperator<Tuple2<String, Integer>> tpr = tp.reduce((ReduceFunction<Tuple2<String, Integer>>) (a, b) -> new Tuple2<>(a.f0, a.f1 + b.f1));
        tpr.print();



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