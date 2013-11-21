package edu.ucla.nesl.sensorsafe.tools;

import java.util.Random;

public class DiffPrivNoiseGenerator {

	private static final double epsilon;

	static {
		// Calculate machine epsilon value;
		float eps = 1.0f;
		do {
			eps /= 2.0f;
		} while ((float) (1.0 + (eps / 2.0)) != 1.0);
		epsilon = eps;
	}

	private Random random = new Random();    	
	private double min;
	private double max;
	private double delta;

	public DiffPrivNoiseGenerator(double min, double max) {
		this.random = new Random();
		this.min = min;
		this.max = max;
		this.delta = Math.abs(max - min);
	}

	public double getAvgNoise(int n) {
		double lambda = epsilon * ( (double)n / delta);
		return getLaplaceRandom(lambda);
	}

	public double getMinMaxNoise() {
		double lambda = epsilon * delta;
		return getLaplaceRandom(lambda);
	}

	public double getMedianNoise() {
		double lambda = epsilon * ( 2.0 / delta );
		return getLaplaceRandom(lambda);
	}

	public double getSumNoise() {
		double lambda = epsilon * Math.max(Math.abs(min), Math.abs(max));
		return getLaplaceRandom(lambda);
	}

	public double getNthNoise(double delta) {
		double lambda = epsilon * ( 1.0 / delta );
		return getLaplaceRandom(lambda);
	}

	private double getLaplaceRandom(double lambda) {
		double mu = 0.0;
		double b = 1.0 / lambda;
		double U = random.nextDouble() - 0.5;
		double X = mu - b * Math.signum(U) * Math.log(1 - 2 * Math.abs(U));
		return X;
	}
}

