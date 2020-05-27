package com.okg.wikipedia.util;

import org.apache.commons.math3.distribution.ZipfDistribution;

public class ZipfDataGenerator {

    // 10^6
    private static final int NUMBER_OF_ELEMENTS = 1000000;
    private static final double z = 2.0;
    private final ZipfDistribution zipfDistribution;

    public ZipfDataGenerator() {
        this(NUMBER_OF_ELEMENTS, z);
    }

    public ZipfDataGenerator(int numberOfElements, double z) {
        zipfDistribution = new ZipfDistribution(numberOfElements, z);
    }

    public int sample() {
        return zipfDistribution.sample();
    }
}
