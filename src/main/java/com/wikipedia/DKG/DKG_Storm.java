package com.wikipedia.DKG;


import com.wikipedia.KeySelector;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.WorkerTopologyContext;

import java.io.Serializable;
import java.util.List;


/**
 * @author Nicolo Rivetti
 */
public class DKG_Storm implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = -8611934642809524449L;

    public final double theta;
    public final double factor;
    public final double epsilonFactor;

    private List<Integer> targetTasks;

    public final int learningLength;
    private int m = 0;
    private DKG_Builder builder;
    private DKGHash hash;
    private KeySelector keySelector;

    /**
     * Implements the CustomStreamGrouping interface offered by the Apache Storm
     * API This implementation leverages the Distribution-Aware Key Grouping
     * (DKG) algorithm
     *
     * @param theta          double value in (0,1], heavy hitter threshold, i.e., all keys
     *                       with an empirical probability larger than or equal to theta
     *                       belong to the heavy hitter set.
     * @param factor         double value >=1, set the number of buckets of sparse items to
     *                       factor * k (number of available instances).
     * @param learningLength number of tuples that will be used to learn the key value
     *                       distribution (these tuples are discarded).
     * @param keySelector            An instance of an implementation of the IKey interface,
     *                       returning an integer value representing the key, through which
     *                       the operator state is partitioned, associated with each tuple
     *                       of the stream.
     * @param epsilonFactor  defines the ration between the Space Saving's Heavy Hitter
     *                       threshold theta and precision parameter epsilon: epsilon =
     *                       theta / epsilonFactor.
     */
    public DKG_Storm(double theta, double factor, int learningLength, KeySelector keySelector, double epsilonFactor) {
        super();
        this.theta = theta;
        this.factor = factor;
        this.epsilonFactor = epsilonFactor;
        this.learningLength = learningLength;
        this.keySelector = keySelector;
    }

    /**
     * Implements the CustomStreamGrouping interface offered by the Apache Storm
     * API This implementation leverages the Distribution-Aware Key Grouping
     * (DKG) algorithm
     *
     * @param theta          double value in (0,1], heavy hitter threshold, i.e., all keys
     *                       with an empirical probability larger than or equal to theta
     *                       belong to the heavy hitter set.
     * @param factor         double value >=1, set the number of buckets of sparse items to
     *                       factor * k (number of available instances).
     * @param learningLength number of tuples that will be used to learn the key value
     *                       distribution (these tuples are discarded).
     * @param keySelector    An instance of an implementation of the IKey interface,
     *                       returning an integer value representing the key, through which
     *                       the operator state is partitioned, associated with each tuple
     *                       of the stream.
     */
    public DKG_Storm(double theta, double factor, int learningLength, KeySelector keySelector) {
        super();
        this.theta = theta;
        this.factor = factor;
        this.epsilonFactor = 2.0;
        this.learningLength = learningLength;
        this.keySelector = keySelector;
    }


    //    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {

        this.targetTasks = targetTasks;

        int k = targetTasks.size();

        this.builder = new DKG_Builder(theta, epsilonFactor, k, factor);

    }



    // learn
    public void learn(List<String> values) {

        this.builder.newSample(keySelector.get(values));
        m++;
        if (m == learningLength) {
            //build
            this.hash = this.builder.build();
        }

    }

    public int chooseTask(List<String> values) {
        return targetTasks.get(this.hash.map(keySelector.get(values)));
    }

}