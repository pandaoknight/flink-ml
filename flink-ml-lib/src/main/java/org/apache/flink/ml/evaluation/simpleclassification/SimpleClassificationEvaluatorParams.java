package org.apache.flink.ml.evaluation.simpleclassification;

import org.apache.flink.ml.common.param.HasLabelCol;
import org.apache.flink.ml.common.param.HasRawPredictionCol;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.ParamValidators;
import org.apache.flink.ml.param.StringArrayParam;

public interface SimpleClassificationEvaluatorParams<T>
        extends HasLabelCol<T>, HasRawPredictionCol<T> {

    String ACURRACY = "accuracy";
    String PRECISION = "precision";
    String RECALL = "recall";
    String F1_SCORE = "f1Score";

    Param<String[]> METRICS_NAMES =
            new StringArrayParam(
                    "metricsNames",
                    "Names of output metrics.",
                    new String[] {ACURRACY},
                    ParamValidators.isSubSet(ACURRACY, PRECISION, RECALL, F1_SCORE));

    default String[] getMetricsNames() {
        return get(METRICS_NAMES);
    }

    default T setMetricsNames(String... value) {
        return set(METRICS_NAMES, value);
    }
}
