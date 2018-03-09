package com.logpoint.analytics.tsanomaly.model;

import java.util.ArrayList;

public class MovingMedian implements PredictiveModel {
    private int windowLen;
    private double smooth = 0;
    private ArrayList<Double> windowQueue;

    /**
     * Default constructor default window Length is set to be 7
     */
    public MovingMedian() {
        windowLen = 7;
        windowQueue = new ArrayList<Double>(windowLen);
    }

    /**
     * Constructor that takes window length as argument
     *
     * @param windowLen : Integer, length of window for median filter, should be >=3
     */
    public MovingMedian(int windowLen) {
        this.windowLen = windowLen;
        windowQueue = new ArrayList<Double>(windowLen);
    }

    /**
     * To push a data from stream to the median filter
     *
     * @param data : Integer from stream to be pushed to moving median filter
     */

    @Override
    public void push(double data) {

        windowQueue.add(data);
        if (windowQueue.size() == 1) {
            smooth = data;
        } else if (windowQueue.size() <= windowLen) {
            smooth = quickSelect(windowQueue, (windowQueue.size() + 1) / 2);
        } else {
            double removed = windowQueue.remove(0);
            // short circuiting
            if (!(removed < smooth && data <= smooth) && !(removed > smooth && data >= smooth)
                    && !(removed == smooth && data == smooth)) {
                smooth = quickSelect(windowQueue, windowQueue.size() / 2 + 1);
            }
        }
    }

    @Override
    public double predict() {
        return smooth;
    }

    /**
     * Quick Select algorithm for finding k^th order statistics of an array
     *
     * @param arr : ArrayList<Integer>, an array for which we have to find order
     *            statistics
     * @param k   : value of order statistics
     * @return the value of k^th order statistic of an array arr
     */

    private double quickSelect(ArrayList<Double> arr, int k) {

        int len = arr.size();
        if (k > len || k <= 0) {
            return -1;
        }

        ArrayList<Double> smaller = new ArrayList<Double>();
        ArrayList<Double> bigger = new ArrayList<Double>();
        int low = 0;
        int high = len - 1;
        int mid = (low + high) / 2;
        double pivot = arr.get(mid);
        for (int i = 0; i < len; i++) {
            if (arr.get(i) < pivot) {
                smaller.add(arr.get(i));
            } else if (arr.get(i) > pivot) {
                bigger.add(arr.get(i));
            }
        }

        if (k > smaller.size() && k <= len - bigger.size()) {
            return pivot;
        } else if (k <= smaller.size()) {
            return quickSelect(smaller, k);
        } else {
            return quickSelect(bigger, k - (len - bigger.size()));
        }
    }

    /**
     * toString method
     *
     * @return the string view of the object
     */
    @Override
    public String toString() {
        return windowQueue.toString() + ", Median: " + smooth;
    }

    public int getWindowLen() {
        return windowLen;
    }

    public void setWindowLen(int windowLen) {
        this.windowLen = windowLen;
    }

    public double getSmooth() {
        return smooth;
    }

    public void setSmooth(double smooth) {
        this.smooth = smooth;
    }

    public ArrayList<Double> getWindowQueue() {
        return windowQueue;
    }

    public void setWindowQueue(ArrayList<Double> windowQueue) {
        this.windowQueue = windowQueue;
    }
}
