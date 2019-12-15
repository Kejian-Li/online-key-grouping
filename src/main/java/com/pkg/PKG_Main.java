package com.pkg;

import com.csvreader.CsvReader;
import com.dkg.CsvItemReader;
import org.apache.commons.math3.util.FastMath;

import java.io.FileNotFoundException;

public class PKG_Main {

    private static final int k = 10;

    public static void main(String[] args) {

        String windowsFileName = "C:\\Users\\lizi\\Desktop\\OKG_Workspace\\OKG_data\\Unfixed_Distribution" +
                "\\zipf_z_unfixed_data.csv";
        String ubuntuFileName = "/home/lizi/workspace/scala_workspace/zipf_data/zipf_z_unfixed_data.csv";

        String inFileName = windowsFileName;
        CsvReader csvReader = null;
        try {
            csvReader = new CsvReader(inFileName);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        CsvItemReader csvItemReader = new CsvItemReader(csvReader);
        String[] items = csvItemReader.nextItem();

        PKG_Partitioner pkg_partitioner = new PKG_Partitioner(k);

        int[] buckets = new int[k];

        int tuplesNum = 0;
        int tuplesLimitation = 200000;
        while (items != null && tuplesNum < tuplesLimitation) {
            for (int i = 0; i < items.length; i++) {
                tuplesNum++;
                int item = Integer.parseInt(items[i]);
//                int targetIndex = twoUniversalHash(item);
                int targetIndex = pkg_partitioner.partition(item);
                buckets[targetIndex] += 1;
            }
            items = csvItemReader.nextItem();
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
