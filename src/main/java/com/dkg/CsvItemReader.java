package com.dkg;

import com.csvreader.CsvReader;

import java.io.IOException;

public class CsvItemReader {

    private CsvReader reader;

    public CsvItemReader(CsvReader reader) {
        this.reader = reader;
    }

    public String[] nextItem() {
        try {
            if (reader.readRecord()) {
                return reader.getValues();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
