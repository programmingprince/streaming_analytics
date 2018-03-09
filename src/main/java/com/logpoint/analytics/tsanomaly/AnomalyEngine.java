package com.logpoint.analytics.tsanomaly;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.logpoint.analytics.tsanomaly.logger.SyslogTCP;
import com.logpoint.analytics.tsanomaly.mergerutils.AnomalyWorkerThread;

import com.immunesecurity.shared.exception.MappingException;
import com.immunesecurity.shared.lib.Log;

import sun.misc.Signal;
import sun.misc.SignalHandler;

public class AnomalyEngine implements SignalHandler {

    public static volatile ConcurrentHashMap<String, AnomalyModel> anomalyModelMap =
            new ConcurrentHashMap<>();
    public static volatile ConcurrentHashMap<String, Long> lastTriggeredMap =
            new ConcurrentHashMap<>();
    private SyslogTCP syslogTCP;

    public AnomalyEngine() {
        this.syslogTCP = new SyslogTCP();
    }

    public void start() throws MappingException {
        cleanUp();
        ExecutorService executorService =
                new ThreadPoolExecutor(5, 10, 5000L, TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<Runnable>(10, true),
                        new ThreadPoolExecutor.CallerRunsPolicy());


        Future<?> future = executorService.submit(new AnomalyWorkerThread(
                this.syslogTCP));
        try {
            future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        executorService.shutdown();
    }


    @Override
    public void handle(Signal paramSignal) {
        cleanUp();
    }

    private void cleanUp() {
        Log.info("AnomalyEngine; started cleanup...");

        Log.info("AnomalyEngine; cleanup completed!!!");
    }

}
