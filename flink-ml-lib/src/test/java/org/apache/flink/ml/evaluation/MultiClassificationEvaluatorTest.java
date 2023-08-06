package org.apache.flink.ml.evaluation;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.ml.evaluation.multiclassification.MultiClassificationEvaluator;
import org.apache.flink.ml.evaluation.multiclassification.MultiClassificationEvaluatorParams;
import org.apache.flink.ml.util.TestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class MultiClassificationEvaluatorTest extends AbstractTestBase {
    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();
    private StreamTableEnvironment tEnv;
    private StreamExecutionEnvironment env;
    private Table inputDataTable;
    private static final List<Row> INPUT_DATA =
            Arrays.asList(
                    Row.of("猫", "猫"),
                    Row.of("猫", "猫"),
                    Row.of("猫", "猫"),
                    Row.of("狗", "猫"),
                    Row.of("狗", "狗"),
                    Row.of("狗", "狗"),
                    Row.of("狗", "狼"),
                    Row.of("狼", "狼"),
                    Row.of("狼", "狗"),
                    Row.of("狼", "狼"));

    private static final double[] EXPECTED_DATA =
            new double[] {
                    0.6, // Macro-average Accuracy
                    0.5, // Macro-average Precision
                    0.6, // Macro-average Recall
                    0.5714285714285714, // Macro-average F1
                    0.7, // Micro-average Accuracy
                    0.7, // Micro-average Precision
                    0.7, // Micro-average Recall
                    0.7}; // Micro-average F1
    private static final double EPS = 1.0e-5;

    @Before
    public void before() {
        env = TestUtils.getExecutionEnvironment();
        //G1)简易解决：java.lang.UnsupportedOperationException: Generic types have been disabled in the ExecutionConfig and type org.apache.flink.ml.evaluation.Multiclassification.MultiClassificationEvaluator$Metrics is treated as a generic type.
        env.getConfig().enableGenericTypes();
        tEnv = StreamTableEnvironment.create(env);
        inputDataTable =
                tEnv.fromDataStream(env.fromCollection(INPUT_DATA)).as("label", "rawPrediction");
    }

    @After
    public void after() throws Exception {
        env.execute();
    }


    @Test
    public void testEvaluate() {
        MultiClassificationEvaluator eval = new MultiClassificationEvaluator()
                .setMetricsNames(
                        "macroAccuracy", //1)如果Param改名字，会导致单测失效
                        "macroPrecision",
                        "macroRecall",
                        "macroF1Score",
                        "microAccuracy",
                        "microPrecision",
                        "microRecall",
                        "microF1Score");

        Table evalResult = eval.transform(inputDataTable)[0];
        List<Row> results = IteratorUtils.toList(evalResult.execute().collect());
        assertArrayEquals(
                new String[] {
                        MultiClassificationEvaluatorParams.MACRO_ACCURACY,
                        MultiClassificationEvaluatorParams.MACRO_PRECISION,
                        MultiClassificationEvaluatorParams.MACRO_RECALL,
                        MultiClassificationEvaluatorParams.MACRO_F1_SCORE,
                        MultiClassificationEvaluatorParams.MICRO_ACCURACY,
                        MultiClassificationEvaluatorParams.MICRO_PRECISION,
                        MultiClassificationEvaluatorParams.MICRO_RECALL,
                        MultiClassificationEvaluatorParams.MICRO_F1_SCORE},
                evalResult.getResolvedSchema().getColumnNames().toArray());
        Row result = results.get(0);
        for (int i = 0; i < EXPECTED_DATA.length; ++i) {
            assertEquals(EXPECTED_DATA[i], result.getFieldAs(i), EPS);
        }
    }

}
