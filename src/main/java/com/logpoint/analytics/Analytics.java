package com.logpoint.analytics;

import com.immunesecurity.shared.exception.MappingException;
import com.immunesecurity.shared.lib.Log;
import com.logpoint.analytics.tsanomaly.AnomalyEngine;

public class Analytics {

    public static void main(String[] args) throws MappingException {
        Log.info("Starting streaming analytics...");

//        AnalyticsJob analyticsJob = new AnalyticsJob();
//        Thread runnableAnalyticsJob = new Thread(analyticsJob);
//        runnableAnalyticsJob.start();

        AnalyticsJobUsingProcessor analyticsJob = new AnalyticsJobUsingProcessor();
        Thread runnableAnalyticsJob = new Thread(analyticsJob);
        runnableAnalyticsJob.start();

        Log.info("Starting anomaly engine...");
        AnomalyEngine anomalyEngine = new AnomalyEngine();
        anomalyEngine.start();
    }
}
