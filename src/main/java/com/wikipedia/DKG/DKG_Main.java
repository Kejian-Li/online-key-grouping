package com.wikipedia.DKG;

import com.reader.WikipediaItemReader;
import com.wikipedia.KeySelector;
import org.apache.commons.math3.util.FastMath;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class DKG_Main {

    public static void main(String[] args) throws IOException {

        String wikipediaFilePath = "C:\\Users\\lizi\\Desktop\\OKG_Workspace\\OKG_data\\Wikipedia_Data"
                + "\\wiki.1191201596.gz";

        BufferedReader in = null;
        try {
            InputStream rawin = new FileInputStream(wikipediaFilePath);
            rawin = new GZIPInputStream(rawin);
            in = new BufferedReader(new InputStreamReader(rawin));
        } catch (FileNotFoundException e) {
            System.err.println("File not found");
            e.printStackTrace();
            System.exit(1);
        }

        WikipediaItemReader wikipediaItemReader = new WikipediaItemReader(in);
        String[] items = wikipediaItemReader.nextItem();

        start(items, wikipediaItemReader);

    }

    private static int k = 32;

    public static void start(String[] items, WikipediaItemReader wikipediaItemReader) {
        double theta = 0.1;
        double mu = 2;
        int learningLength = 80000;

        KeySelector iKey = new KeySelector() {
            @Override
            public String get(List<String> values) {
                return values.get(2);  // url of wikipedia data as key
            }
        };


        DKG_Storm dkg_storm = new DKG_Storm(theta, mu, learningLength, iKey);
        List<Integer> targetTasks = new ArrayList<>(k);
        for (int i = 0; i < k; i++) {
            targetTasks.add(i);
        }

        dkg_storm.prepare(null, null, targetTasks);

        int m = 0;
        // learn

        while (items != null && m < learningLength) {
            ArrayList<String> tuple = new ArrayList<>();
            for (int i = 0; i < items.length; i++) {
                tuple.add(items[i]);
            }
            dkg_storm.learn(tuple);
            m++;
            items = wikipediaItemReader.nextItem();
        }
        System.out.println("DKG learns " + m + " tuples");

        int[] buckets = new int[k];

        m = 0;
        wikipediaItemReader.nextItem();
        items = wikipediaItemReader.nextItem();

        // assign
        int N = 120000;
        while (items != null && m < N) {
                List<String> tuple = new ArrayList<>();
            for (int i = 0; i < items.length; i++) {
                tuple.add(items[i]);
            }
            int targetIndex = dkg_storm.chooseTask(tuple);

            buckets[targetIndex] += 1;
            m++;
            items = wikipediaItemReader.nextItem();
        }
        System.out.println("DKG assigns " + m + " tuples");

        int loadSum = buckets[0];
        int maxLoad = buckets[0];
        System.out.println("buckets 0: " + buckets[0]);
        for (int i = 1; i < k; i++) {
            loadSum += buckets[i];
            if (maxLoad < buckets[i]) {
                maxLoad = buckets[i];
            }
            System.out.println("buckets " + i + ": " + buckets[i]);
        }
        int averageLoad = loadSum / k;
        System.out.println("Average load is " + averageLoad);
        double imbalance = ((maxLoad / (double) averageLoad) - 1) * 100;
        System.out.println("DKG's imbalance is " + imbalance + "%");


        long squareSum = 0;
        for (int i = 0; i < k; i++) {
            int difference = buckets[i] - averageLoad;
            double square = FastMath.pow((double) difference, 2);
            squareSum += square;
        }
        double sigma = Math.sqrt(squareSum / k);
        System.out.println("Final standard deviation is " + sigma);
    }

}
