package com.logpoint.analytics.tsanomaly.model;

import java.io.Serializable;

public interface PredictiveModel extends Serializable {
	/**
	 * method for feeding data into model
	 * 
	 * @param data
	 */
	public void push(double data);

	/**
	 * method to return the value predicted by model
	 * 
	 * @return prediction
	 */
	public double predict();
}
