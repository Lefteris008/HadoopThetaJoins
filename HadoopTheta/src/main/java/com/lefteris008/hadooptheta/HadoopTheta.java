package com.lefteris008.hadooptheta;

import com.lefteris008.hadooptheta.countphase.CounterMap;
import com.lefteris008.hadooptheta.countphase.CounterReduce;
import com.lefteris008.hadooptheta.thetaphase.ThetaJoinReduce;
import com.lefteris008.hadooptheta.thetaphase.ThetaJoinMap;
import com.lefteris008.hadooptheta.partitioner.MatrixToReducerPartitioning;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 
 * @author  Lefteris Paraskevas
 * @author  Alexandros Tzanakas
 * @version 2016.05.13_2359
 */
public class HadoopTheta {
	
    public static void main(String args[]) throws IOException, ClassNotFoundException, 
            InterruptedException {

        //Initialize time
        long startTime = System.currentTimeMillis();
        System.out.println("First MR Phase for Theta Join started at " 
                + startTime / 1000 + "\n");

        //Initialize cardinalities, the number of reducers
        //and the input/output file locations
        int cardinalityOfS = Integer.parseInt(args[0]);
        int cardinalityOfR = Integer.parseInt(args[1]);
        int numberOfReducers = Integer.parseInt(args[2]);
        String inputFileLocation = args[3];
        String outputFileLocation = args[4];
        String tempFileLocation = "/tmp/outputForThetaJoin/";

        Configuration conf = new Configuration();

        //"Unlock" the limit of the input split files
        //This helps for large input files (that exceed 1 million lines)
        conf.set("mapreduce.jobtracker.split.metainfo.maxsize", "-1");

        //Create the MatrixToReducerPartitioning object, serialize it and store 
        //it into a file
        MatrixToReducerPartitioning mtr = 
                new MatrixToReducerPartitioning(cardinalityOfS, cardinalityOfR,
                        numberOfReducers);
        mtr.Partitioner();

        try {
            FileOutputStream fileOut = new FileOutputStream("/tmp/matrixpartitioner.ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(mtr);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
        }

        //*******************************************************
        //First Map/Reduce Phase (Theta-Join applying the filter)
        //*******************************************************

        //Set the job as 'thetajoin'
        Job job = new Job(conf, "thetajoin");
        
        //Set the number of reducers
        job.setNumReduceTasks(numberOfReducers);
        
        //Set the classes
        job.setJarByClass(HadoopTheta.class);
        job.setMapperClass(ThetaJoinMap.class);
        job.setReducerClass(ThetaJoinReduce.class);
               
        //Set the input/output classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        //Set the input/output format classes
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        //Set the input/output file paths
        FileInputFormat.addInputPath(job, new Path(inputFileLocation));
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(tempFileLocation), true);
        FileOutputFormat.setOutputPath(job, new Path(tempFileLocation));
        
        boolean result = job.waitForCompletion(true);
        
        //Show the time in milliseconds that the first MR phase was running
        long endTime = System.currentTimeMillis();
        System.out.println("\nFirst MR Phase for Theta-Join ended at " 
                + endTime / 100);
        System.out.println("First MR Phase for Theta-Join run for " 
                + (endTime - startTime) + " milliseconds");
		
        if(result) {
            System.out.println("First MR Phase for Theta-Join completed succesfully.\n");
        } else {
            System.out.println("First MR Phase for Theta-Join failed.\n");
            System.exit(1);
        }
		
        //************************************************************************
        //Second Map/Reduce Phase (Counting the S.x attribute and grouping by R.a)
        //************************************************************************
		
        //Initialize the time for the second phase
        startTime = System.currentTimeMillis();
        System.out.println("\nSecond MR Phase for counting the S.x "
                + "attributes started at " + startTime + "\n");

        //Reinitialize configuration
        conf = new Configuration();

        //Set the job as 'count'
        job = new Job(conf, "count");
        
        //Set the classes
        job.setJarByClass(HadoopTheta.class);
        job.setMapperClass(CounterMap.class);
        job.setReducerClass(CounterReduce.class);
        
        //Set the input/output classes
        //Note that in this case, Map outputs a key that is IntWritable
        //and not Text (and so, the reducer receives an IntWritable key)
        //This helps to group the R.a values numerically. Otherwise, the
        //R.a key is sorted lexicographically and for values greater than
        //100, the final grouping is displayed wrong
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        //Set the input/output format classes
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        //Set the input/output file paths
        //In that case, Mapper has to read from the previous
        //Reducer outputs and exports its results to the user-defined output
        FileInputFormat.addInputPath(job, new Path(tempFileLocation));

        //Command to overwrite the output folder if it already exists
        fs.delete(new Path(outputFileLocation), true);
        
        //Store the output
        FileOutputFormat.setOutputPath(job, new Path(outputFileLocation));
        
        result = job.waitForCompletion(true);
        endTime = System.currentTimeMillis();
		
        if(result) {
            System.out.println("\nSecond MR Phase for counting the S.x "
                    + "attributes endned at " + endTime);
            System.out.println("Second MR Phase for counting the S.x "
                    + "attributes run for: " + (endTime - startTime) + " milliseconds");
            System.out.println("Second MR Phase for counting the S.x "
                    + "attributes completed successfully.");
            System.out.println("\nProject run for " + (endTime - startTime) 
                    + " milliseconds");
        }else {
            System.out.println("\nSecond MR Phase for counting the S.x "
                    + "attributes endned at " + endTime);
            System.out.println("Second MR Phase for counting the S.x "
                    + "attributes run for: " + (endTime - startTime) + " milliseconds");
            System.out.println("Second MR Phase for counting the S.x attributes failed.");
            System.out.println("\nProject run for " + (endTime - startTime) + " milliseconds");
            System.exit(1);
        }
    }
}