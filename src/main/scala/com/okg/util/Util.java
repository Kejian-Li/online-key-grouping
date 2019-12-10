package com.okg.util;

import org.apache.commons.math3.util.FastMath;

public class Util {

    public static void main(String[] args) {
        int N = 10000000;
        double theta = 0.01;
        double z = 2.0;
        double harmonicNumber = generalizedHarmonic(N, z);
        double K = FastMath.pow(theta * harmonicNumber, (-1) / z);
        System.out.println("Size of Routing Table is proximate to " + K);
    }

    /**
     * Calculates the Nth generalized harmonic number. See
     * <a href="http://mathworld.wolfram.com/HarmonicSeries.html">Harmonic
     * Series</a>.
     *
     * @param n Term in the series to calculate (must be larger than 1)
     * @param m Exponent (special case {@code m = 1} is the harmonic series).
     * @return the n<sup>th</sup> generalized harmonic number.
     */
    private static double generalizedHarmonic(final int n, final double m) {
        double value = 0;
        for (int k = n; k > 0; --k) {
            value += 1.0 / FastMath.pow(k, m);
        }
        return value;
    }
}
