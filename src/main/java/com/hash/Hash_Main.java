package com.hash;

import com.csvreader.CsvReader;
import com.dkg.CsvItemReader;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.okg.util.TwoUniversalHash;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.io.FileNotFoundException;

public class Hash_Main {

    private static final int k = 10;
    private static TwoUniversalHash twoUniversalHash;
    private static HashFunction murmurHash;

    public static void main(String[] args) {
        String windowsFileName = "C:\\Users\\lizi\\Desktop\\thesis_workspace\\OKG_workspace\\OKG_data\\" +
                "Zipf_Data\\Fixed_Distribution\\zipf_z_2-8.csv";
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

        twoUniversalHash = initializeTwoUniversalHash();

        murmurHash = Hashing.murmur3_128(13);

        int[] buckets = new int[k];

        while(items != null) {
            for (int i = 0; i < items.length; i++) {
                int item = Integer.parseInt(items[i]);
//                int targetIndex = twoUniversalHash(item);
                int targetIndex = murmurHash(item);
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
        System.out.println("HASH's imbalance is " + imbalance + "%");
    }

    private static TwoUniversalHash initializeTwoUniversalHash() {
        RandomDataGenerator uniformGenerator = new RandomDataGenerator();
        uniformGenerator.reSeed(1000);

        long prime = 10000019L;
        long a = uniformGenerator.nextLong(1, prime - 1);
        long b = uniformGenerator.nextLong(1, prime - 1);
        return new TwoUniversalHash(k, prime, a, b);
    }

    public static int twoUniversalHash(int key) {
        return twoUniversalHash.hash(key);
    }

    public static int murmurHash(int key) {
        return Math.abs(murmurHash.hashInt(key).asInt()) % k;
    }
}
