package com.logpoint.analytics.tsanomaly.delta;

import java.io.Serializable;

public interface RunningStats extends Serializable {
	void clear();

	void push(double x);

	long numDataPoints();

	double mean();

	double variance();

	double std();
}
