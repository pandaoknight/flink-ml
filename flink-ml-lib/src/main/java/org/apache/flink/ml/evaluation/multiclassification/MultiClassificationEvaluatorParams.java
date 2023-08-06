package org.apache.flink.ml.evaluation.multiclassification;

import org.apache.flink.ml.common.param.HasLabelCol;
import org.apache.flink.ml.common.param.HasRawPredictionCol;
import org.apache.flink.ml.common.param.HasWeightCol;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.ParamValidators;
import org.apache.flink.ml.param.StringArrayParam;

public interface MultiClassificationEvaluatorParams<T>
        extends HasLabelCol<T>, HasRawPredictionCol<T> {
    String MACRO_ACCURACY = "macroAccuracy";
    String MACRO_PRECISION = "macroPrecision";
    String MACRO_RECALL = "macroRecall";
    String MACRO_F1_SCORE = "macroF1Score";
    String MICRO_ACCURACY = "microAccuracy";
    String MICRO_PRECISION = "microPrecision";
    String MICRO_RECALL = "microRecall";
    String MICRO_F1_SCORE = "microF1Score";

    /**
     * Param for supported metric names in multi-classification evaluation
     */
    Param<String[]> METRICS_NAMES =
            new StringArrayParam(
                    "metricsNames",
                    "Names of output metrics.",
                    new String[] {
                            MACRO_ACCURACY, MACRO_PRECISION, MACRO_RECALL, MACRO_F1_SCORE,
                            MICRO_ACCURACY, MICRO_PRECISION, MICRO_RECALL, MICRO_F1_SCORE
                    },
                    ParamValidators.isSubSet(
                            MACRO_ACCURACY, MACRO_PRECISION, MACRO_RECALL, MACRO_F1_SCORE,
                            MICRO_ACCURACY, MICRO_PRECISION, MICRO_RECALL, MICRO_F1_SCORE
                    ));

    default String[] getMetricsNames() {
        return get(METRICS_NAMES);
    }

    default T setMetricsNames(String... value) {
        return set(METRICS_NAMES, value);
    }
}