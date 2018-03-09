package com.logpoint.analytics.tsanomaly;

import java.io.Serializable;

import org.apache.commons.math3.special.Erf;
import com.logpoint.analytics.tsanomaly.delta.RunningStats;
import com.logpoint.analytics.tsanomaly.model.PredictiveModel;

public class AnomalyModel implements Serializable {
	private static final long serialVersionUID = 6348409949376278249L;
	private static final double EPS = 1e-6;
	private PredictiveModel model;
	private RunningStats delta;
	private double threshold;
	private int neglects = 7;
	private int counter = 0;

	public AnomalyModel(PredictiveModel model, RunningStats delta) {
		this.delta = delta;
		this.model = model;
		this.threshold = -Math.log(0.001);
	}

	public AnomalyModel(PredictiveModel model, RunningStats delta,
			double alpha) {
		this.delta = delta;
		this.model = model;

		if (alpha >= 0.1 || alpha < 0f) {
			alpha = 0.005;
		}
		this.threshold = -Math.log(alpha / 2f);
	}

	public static double getEPS() {
		return EPS;
	}

	public void setNeglect(int n) {
		this.neglects = n;
	}

	/**
	 * @param data
	 *            : an entry from the stream
	 * @return anomaly_score : Anomaly score due to our algorithm for the stream
	 *         entity provided
	 */
	public double push(double data) {
		// increase the counter
		++counter;
		if (data == Double.NaN) {
			data = 0d;
		}

		// get the predicted value by moving median model
		double predicted = model.predict();

		// push to moving median model
		model.push(data);

		// push the residual to the running stats model or t-digest
		double residual = data - predicted;
		double mu = delta.mean();
		double var = delta.variance();

		delta.push(residual);

		// if number of streams so far is smaller than window length, anomaly
		// score is 0
		if (counter < neglects) {
			return 0d;
		}

		// else compute the p-value bases anomaly score based on residual due to
		double p_value =
				Erf.erfc(Math.abs(residual - mu) / Math.pow(2.0f * var, .5));
		double anomaly_score = -Math.log(p_value);
		anomaly_score = ((anomaly_score > 1000) ? 100d
				: anomaly_score / 10 * (anomaly_score > threshold ? 1 : 0));
		// Log.debug(this.hashCode() + " x: " + data + " res: " + residual + "
		// n: "
		// + delta.numDataPoints() + " mu: " + mu + " var: " + var
		// + " as: " + anomaly_score);
		return anomaly_score;
	}

	public PredictiveModel getModel() {
		return model;
	}

	public void setModel(PredictiveModel model) {
		this.model = model;
	}

	public RunningStats getDelta() {
		return delta;
	}

	public void setDelta(RunningStats delta) {
		this.delta = delta;
	}

	public double getThreshold() {
		return threshold;
	}

	public void setThreshold(double threshold) {
		this.threshold = threshold;
	}

	public int getNeglects() {
		return neglects;
	}

	public void setNeglects(int neglects) {
		this.neglects = neglects;
	}

	public int getCounter() {
		return counter;
	}

	public void setCounter(int counter) {
		this.counter = counter;
	}
}
