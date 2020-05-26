package com.wikipedia;

import com.pkg.PKG_Partitioner;
import com.reader.WikipediaItemReader;
import org.apache.commons.math3.util.FastMath;

import java.io.*;
import java.util.zip.GZIPInputStream;

public class PKG_Main {

    private static final int k = 64;

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

    public static void start(String[] items, WikipediaItemReader wikipediaItemReader) {
        PKG_Partitioner pkg_partitioner = new PKG_Partitioner(k);

        int[] buckets = new int[k];

        int tuplesNum = 0;
        int tuplesLimitation = 200000;
        while (items != null && tuplesNum < tuplesLimitation) {
            for (int i = 0; i < items.length; i++) {
                tuplesNum++;
                String url = items[i];
//                int targetIndex = twoUniversalHash(item);
                int targetIndex = pkg_partitioner.partition(url);
                buckets[targetIndex] += 1;
            }
            items = wikipediaItemReader.nextItem();
        }

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
        System.out.println("PKG's imbalance is " + imbalance + "%");


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
