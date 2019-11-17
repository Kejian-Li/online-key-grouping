package com.okg.main

import com.csvreader.CsvReader

class CsvItemReader(reader: CsvReader) {

  def nextItem() = {
    if(reader.readRecord()) {
      reader.getValues()
    }else {
      null
    }
  }

}
