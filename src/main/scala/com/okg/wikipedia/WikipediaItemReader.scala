package com.okg.wikipedia

import java.io.{BufferedReader, IOException}


class WikipediaItemReader(input: BufferedReader) {

  def nextItem(): Array[String] = {

    var line = String.valueOf(0)

    try
      line = input.readLine
    catch {
      case e: IOException =>
        System.err.println("Unable to read from file")
        e.printStackTrace()
    }


    val fields = line.split(" ")

    return fields
  }

}
