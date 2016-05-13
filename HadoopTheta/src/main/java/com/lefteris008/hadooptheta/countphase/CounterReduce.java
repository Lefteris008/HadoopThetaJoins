package com.lefteris008.hadooptheta.countphase;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Lefteris Paraskevas
 * @author Alexandros Tzanakas
 * @version 2016.05.14_0028
 */
public class CounterReduce extends Reducer<IntWritable, Text, Text, Text> {

    /**
     * This reducer sums all the values that it will receive (which are the
     * semi-summaries of the S.x attribute, calculated from the previous
     * Map/Reduce phase) for a single key and outputs the summaries, grouped by
     * the R.a attribute as the key
     *
     * @param key An IntWritable key for the reduce process
     * @param values An Iterable list containing Text values for the reduce
     * process
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public final void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int rA_Attribute = key.get();
        BigInteger countX = new BigInteger("0");
        BigInteger tempSum;
        while (values.iterator().hasNext()) {
            
            //Iterate through the list of values (the semi-summaries of the
            //S.x attribute) and sum every single value
            tempSum = new BigInteger(values.iterator().next().toString());
            countX = countX.add(tempSum);
        }

        //Output the R.a attribute (the input key) followed by the full summary
        //of the S.x attribute values that the R.a has been joined
        //As previously, 'null' is used as the output key, to store all
        //the outputs of every reducer to a single file
        context.write(null, new Text(rA_Attribute + "\t" + countX.toString()));
    }

}
