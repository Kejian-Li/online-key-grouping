package com.dkg;

import com.csvreader.CsvReader;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

public class DKG_Main {

    public static void main(String[] args) {

        String windowsFileName = "C:\\Users\\lizi\\Desktop\\分布式流处理系统的数据分区算法研究\\dataset\\zipf_dataset\\zipf_z_2-0.csv";
        String ubuntuFileName = "/home/lizi/workspace/scala_workspace/zipf_data/zipf_z_2-0.csv";

        String inFileName = windowsFileName;
        CsvReader csvReader = null;
        try {
            csvReader = new CsvReader(inFileName);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        CsvItemReader csvItemReader = new CsvItemReader(csvReader);
        String[] item = csvItemReader.nextItem();

        double theta = 0.1;
        double mu = 2;
        int learningLength = 80000;
        IKey iKey = new IKey() {
            @Override
            public int get(List<Object> values) {
                return Integer.parseInt(values.get(0).toString());
            }
        };


        int k = 10;

        DKG_Storm dkg_storm = new DKG_Storm(theta, mu, learningLength, iKey);
        List<Integer> targetTasks = new ArrayList<>(k);
        for (int i = 0; i < k; i++) {
            targetTasks.add(i);
        }

        dkg_storm.prepare(null, null, targetTasks);

        int m = 0;
        int M = learningLength;
        int N = 2000000;
        while (item != null && m < M) {
            for (int i = 0; i < item.length; i++) {
                List<Object> tuple = new ArrayList<>(1);
                tuple.add(item[i]);
                dkg_storm.chooseTasks(-1, tuple);
                m++;
                System.out.println("DKG learns " + m + " tuples");
            }
            item = csvItemReader.nextItem();
        }

        int[] buckets = new int[k];

        m = 0;

        csvItemReader.nextItem();
        String[] items = csvItemReader.nextItem();
        while (items != null && m < N) {
            for (int i = 0; i < items.length; i++) {
                List<Object> tuple = new ArrayList<>(1);
                tuple.add(items[i]);
                List<Integer> target = dkg_storm.chooseTasks(-1, tuple);
                int targetIndex = target.get(0);
                buckets[targetIndex] += 1;
                m++;
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
        System.out.println("DKG's imbalance is " + imbalance + "%");
    }
}
