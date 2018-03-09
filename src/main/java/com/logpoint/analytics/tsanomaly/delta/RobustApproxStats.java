package com.logpoint.analytics.tsanomaly.delta;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import com.tdunning.math.stats.AVLTreeDigest;

public class RobustApproxStats implements RunningStats {
	private static final long serialVersionUID = 8333734529672556586L;
	private double EPS = 1e-6;
	private AVLTreeDigest td;

	public RobustApproxStats() {
		clear();
	}

	@Override
	public void clear() {
		td = new AVLTreeDigest(500);
	}

	@Override
	public void push(double x) {
		td.add(x);
	}

	@Override
	public long numDataPoints() {
		return td.size();
	}

	@Override
	public double mean() {
		return td.quantile(0.5);
	}

	public double getPercentile(Double n) {
		return td.cdf(n);
	}

	@Override
	public double variance() {
		return std() * std();
	}

	@Override
	public double std() {
		return Math.max(0.74 * (td.quantile(0.75) - td.quantile(0.25)), EPS);
	}

	public AVLTreeDigest getTd() {
		return td;
	}

	public void setTd(AVLTreeDigest td) {
		this.td = td;
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		byte[] byteArray = new byte[td.byteSize()];
		out.writeInt(byteArray.length);
		td.asBytes(ByteBuffer.wrap(byteArray));
		out.write(byteArray);
	}

	private void readObject(ObjectInputStream in)
			throws IOException, ClassNotFoundException {
		int byteLength = in.readInt();
		byte[] byteArray = new byte[byteLength];
		in.readFully(byteArray);
		td = AVLTreeDigest.fromBytes(ByteBuffer.wrap(byteArray));
	}
}
