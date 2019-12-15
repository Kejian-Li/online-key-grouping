package com.pkg;

public interface StreamPartitioner {

    /**
     *
     * @param key
     * @return index of selected downstream operator.
     */
    int partition(Object key) throws Exception;

    /**
     *
     * @return
     */
    String getName();

}
