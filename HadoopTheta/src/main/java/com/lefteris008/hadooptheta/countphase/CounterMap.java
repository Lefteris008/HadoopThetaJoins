package com.lefteris008.hadooptheta.countphase;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author  Lefteris Paraskevas
 * @author  Alexandros Tzanakas
 * @version 2016.05.14_0027
 */
public class CounterMap extends Mapper<LongWritable, Text, IntWritable, Text> {
	
    /**
     * This Mapper reads the output file of the previous Map/Reduce phase and
     * outputs the attributes that will find in it to the reducer, by splitting
     * them (the first will be the key and the second will be the value).
     *
     * @param key A LongWritable key for the mapping process.
     * @param value A Text value for the mapping process.
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public final void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        //Split the two arguments from the input file
        String line = value.toString();
        String[] attributes = line.split(",");

        //Output to reducer R.a attribute as the key and
        //the semi-summary of S.x attribute as the value
        context.write(new IntWritable(Integer.parseInt(attributes[0])), 
                new Text(attributes[1]));
    }

}
