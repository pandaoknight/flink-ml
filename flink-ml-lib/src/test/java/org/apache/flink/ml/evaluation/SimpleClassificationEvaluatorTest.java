package org.apache.flink.ml.evaluation;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.ml.evaluation.simpleclassification.SimpleClassificationEvaluator;
import org.apache.flink.ml.evaluation.simpleclassification.SimpleClassificationEvaluatorParams;
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

public class SimpleClassificationEvaluatorTest extends AbstractTestBase {
    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();
    private StreamTableEnvironment tEnv;
    private StreamExecutionEnvironment env;
    private Table inputDataTable;
    private static final List<Row> INPUT_DATA =
        Arrays.asList(
            Row.of(1.0, 1.0), //tp
            Row.of(1.0, 1.0), //tp
            Row.of(1.0, 1.0), //tp
            Row.of(0.0, 1.0), //fp
            Row.of(0.0, 1.0), //fp
            Row.of(1.0, 1.0), //tp
            Row.of(1.0, 1.0), //tp
            Row.of(0.0, 0.0), //tn
            Row.of(0.0, 0.0), //tn
            Row.of(1.0, 0.0), //tn
            Row.of(0.0, 0.0), //tn
            Row.of(1.0, 0.0)); // fn

    private static final double[] EXPECTED_DATA =
            new double[] {0.666666, 0.714285, 0.714285, 0.714285};
    private static final double EPS = 1.0e-5;

    @Before
    public void before() {
        env = TestUtils.getExecutionEnvironment();
        //G1)简易解决：java.lang.UnsupportedOperationException: Generic types have been disabled in the ExecutionConfig and type org.apache.flink.ml.evaluation.simpleclassification.SimpleClassificationEvaluator$Metrics is treated as a generic type.
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
        SimpleClassificationEvaluator eval =
            new SimpleClassificationEvaluator()
                .setMetricsNames(
                    SimpleClassificationEvaluatorParams.ACURRACY,
                    SimpleClassificationEvaluatorParams.PRECISION,
                    SimpleClassificationEvaluatorParams.RECALL,
                    SimpleClassificationEvaluatorParams.F1_SCORE);
        Table evalResult = eval.transform(inputDataTable)[0];
        List<Row> results = IteratorUtils.toList(evalResult.execute().collect());
//        assertArrayEquals(
//            new String[] {
//                SimpleClassificationEvaluatorParams.ACURRACY,
//                SimpleClassificationEvaluatorParams.PRECISION,
//                SimpleClassificationEvaluatorParams.RECALL,
//                SimpleClassificationEvaluatorParams.F1_SCORE},
//            evalResult.getResolvedSchema().getColumnNames().toArray());
//        Row result = results.get(0);
//        for (int i = 0; i < EXPECTED_DATA.length; ++i) {
//            assertEquals(EXPECTED_DATA[i], result.getFieldAs(i), EPS);
//        }
    }

//    @Test
//    public void testParam() {
//        SimpleClassificationEvaluator SimpleEval = new SimpleClassificationEvaluator();
//        assertEquals("label", SimpleEval.getLabelCol());
//        assertNull(SimpleEval.getWeightCol());
//        assertEquals("rawPrediction", SimpleEval.getRawPredictionCol());
//        assertArrayEquals(
//                new String[] {
//                    SimpleClassificationEvaluatorParams.AREA_UNDER_ROC,
//                    SimpleClassificationEvaluatorParams.AREA_UNDER_PR
//                },
//                SimpleEval.getMetricsNames());
//        SimpleEval
//                .setLabelCol("labelCol")
//                .setRawPredictionCol("raw")
//                .setMetricsNames(SimpleClassificationEvaluatorParams.AREA_UNDER_ROC)
//                .setWeightCol("weight");
//        assertEquals("labelCol", SimpleEval.getLabelCol());
//        assertEquals("weight", SimpleEval.getWeightCol());
//        assertEquals("raw", SimpleEval.getRawPredictionCol());
//        assertArrayEquals(
//                new String[] {SimpleClassificationEvaluatorParams.AREA_UNDER_ROC},
//                SimpleEval.getMetricsNames());
//    }
//
//    @Test
//    public void testInputTypeConversion() {
//        inputDataTable = TestUtils.convertDataTypesToSparseInt(tEnv, inputDataTable);
//        assertArrayEquals(
//                new Class<?>[] {Integer.class, SparseVector.class},
//                TestUtils.getColumnDataTypes(inputDataTable));
//
//        SimpleClassificationEvaluator eval =
//                new SimpleClassificationEvaluator()
//                        .setMetricsNames(
//                                SimpleClassificationEvaluatorParams.AREA_UNDER_PR,
//                                SimpleClassificationEvaluatorParams.KS,
//                                SimpleClassificationEvaluatorParams.AREA_UNDER_ROC);
//        Table evalResult = eval.transform(inputDataTable)[0];
//        List<Row> results = IteratorUtils.toList(evalResult.execute().collect());
//        assertArrayEquals(
//                new String[] {
//                    SimpleClassificationEvaluatorParams.AREA_UNDER_PR,
//                    SimpleClassificationEvaluatorParams.KS,
//                    SimpleClassificationEvaluatorParams.AREA_UNDER_ROC
//                },
//                evalResult.getResolvedSchema().getColumnNames().toArray());
//        Row result = results.get(0);
//        for (int i = 0; i < EXPECTED_DATA.length; ++i) {
//            assertEquals(EXPECTED_DATA[i], result.getFieldAs(i), EPS);
//        }
//    }
//
//
//    @Test
//    public void testMoreSubtaskThanData() {
//        List<Row> inputData =
//                Arrays.asList(
//                        Row.of(1.0, Vectors.dense(0.1, 0.9)),
//                        Row.of(0.0, Vectors.dense(0.9, 0.1)));
//        double[] expectedData = new double[] {1.0, 1.0, 1.0};
//        inputDataTable =
//                tEnv.fromDataStream(env.fromCollection(inputData)).as("label", "rawPrediction");
//
//        SimpleClassificationEvaluator eval =
//                new SimpleClassificationEvaluator()
//                        .setMetricsNames(
//                                SimpleClassificationEvaluatorParams.AREA_UNDER_PR,
//                                SimpleClassificationEvaluatorParams.KS,
//                                SimpleClassificationEvaluatorParams.AREA_UNDER_ROC);
//        Table evalResult = eval.transform(inputDataTable)[0];
//        List<Row> results = IteratorUtils.toList(evalResult.execute().collect());
//        assertArrayEquals(
//                new String[] {
//                    SimpleClassificationEvaluatorParams.AREA_UNDER_PR,
//                    SimpleClassificationEvaluatorParams.KS,
//                    SimpleClassificationEvaluatorParams.AREA_UNDER_ROC
//                },
//                evalResult.getResolvedSchema().getColumnNames().toArray());
//        Row result = results.get(0);
//        for (int i = 0; i < expectedData.length; ++i) {
//            assertEquals(expectedData[i], result.getFieldAs(i), EPS);
//        }
//    }
//    @Test
//    public void testSaveLoadAndEvaluate() throws Exception {
//        SimpleClassificationEvaluator eval =
//                new SimpleClassificationEvaluator()
//                        .setMetricsNames(
//                                SimpleClassificationEvaluatorParams.AREA_UNDER_PR,
//                                SimpleClassificationEvaluatorParams.KS,
//                                SimpleClassificationEvaluatorParams.AREA_UNDER_ROC);
//        SimpleClassificationEvaluator loadedEval =
//                TestUtils.saveAndReload(
//                        tEnv,
//                        eval,
//                        tempFolder.newFolder().getAbsolutePath(),
//                        SimpleClassificationEvaluator::load);
//        Table evalResult = loadedEval.transform(inputDataTable)[0];
//        assertArrayEquals(
//                new String[] {
//                    SimpleClassificationEvaluatorParams.AREA_UNDER_PR,
//                    SimpleClassificationEvaluatorParams.KS,
//                    SimpleClassificationEvaluatorParams.AREA_UNDER_ROC
//                },
//                evalResult.getResolvedSchema().getColumnNames().toArray());
//        List<Row> results = IteratorUtils.toList(evalResult.execute().collect());
//        Row result = results.get(0);
//        for (int i = 0; i < EXPECTED_DATA.length; ++i) {
//            assertEquals(EXPECTED_DATA[i], result.getFieldAs(i), EPS);
//        }
//    }
}
