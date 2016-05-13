package com.lefteris008.hadooptheta.thetaphase;

import com.lefteris008.hadooptheta.utilities.Utilities;
import com.lefteris008.hadooptheta.partitioner.MatrixToReducerPartitioning;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author  Lefteris Paraskevas
 * @author  Alexandros Tzanakas
 * @version 2016.05.14_0034
 */
public class ThetaJoinMap extends Mapper<LongWritable, Text, Text, Text> {

    //Global MatrixPartitoner object that will contain
    //the partitioned matrix for use inside the Mapper method
    private MatrixToReducerPartitioning mtr;

    /**
     * This Mapper reads from the input file (line by line) supplied by the Main class
     * a tuple, assigns it to a specific random index in the Matrix created by 
     * the MatrixPartioner class (to a row if it is a R-tuple or to a column if 
     * it is a S-tuple), stores all the regions (aka reducers) that are assigned 
     * to that row/column previously and outputs that tuple to the specific reducers 
     * found previously (after applying it to the querry's filter [S.a > 10])
     * 
     * ==Optimization==
     * The filter of the query applies to the R.a attribute only and prunes the 
     * R-tuples that are lower than or equal to 10. The join condition, on the 
     * other hand, joins R-tuples if only the R.a attribute is lower than or equal 
     * the S.a attribute. If someone solves the double inequality, easily comes 
     * up with the statement that the S.a must be greater than 10 too (R.a > 10 
     * and R.a < S.a => S.a > 10). So, we applied the filter to the S-tuples too 
     * in order to reduce the amount of them that it is supplied to the reducer
     * 
     * @param key A LongWritable key for the mapping process
     * @param value A Text value for the mapping process
     * @param context
     * @throws IOException
     * @throws InterruptedException 
     */
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {

        //Key is not used
        //Get a line of the file
        String line = value.toString();
        List<String> tuple = new ArrayList<>();

        //Extract the tuple of the line, ignoring the commas
        tuple = Utilities.extractTupleFromLine(line);

        //Deserialize the MatrixToReducerPartitioning object created from Main class
        try {
            FileInputStream fileIn = new FileInputStream("/tmp/matrixpartitioner.ser");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            this.mtr = (MatrixToReducerPartitioning) in.readObject();
            in.close();
            fileIn.close();
        } catch (IOException i) {
            i.printStackTrace();
            return;
        } catch (ClassNotFoundException c) {
            System.out.println("MatrixToReducerPartitioning class not found");
            c.printStackTrace();
            return;
        }

        int cardinality;
        List<Integer> regionIDs = new ArrayList<>();
        int tupleLocationInMatrix;

        //Find if the tuple is either from S or R relation
        if (tuple.get(0).equals("S")) {

            //Get the cardinality of the tuple's relation
            cardinality = mtr.getSCardinality();

            //Calculate a random number between 1 and the cardinality
            //(The correct index bounds [0 - (cardinality-1)] are defined
            //in the MatrixtoReducerPartitioner class)
            tupleLocationInMatrix = Utilities.randInt(1, (cardinality));

            //Store all regions that intersect with that column
            regionIDs = mtr.getIntersectingRows(tupleLocationInMatrix);
        } else { //R relation
            cardinality = mtr.getRCardinality();
            tupleLocationInMatrix = Utilities.randInt(1, (cardinality));
            regionIDs = mtr.getIntersectingColumns(tupleLocationInMatrix);
        }

        Text outputKey = null;
        Text outputValue = null;
        boolean filterFail = false;

        //Create the outputValue
        if (tuple.get(0).equals("S")) {
            //Set a boolean flag to true, if the aforementioned S-tuple fails
            //to pass the filter (is lower than or equal to 10) and do not create
            //any outputValue instance
            //(See the optimization note at the top)
            if (Integer.parseInt(tuple.get(1)) <= 10) {
                filterFail = true;
            } else {
                //If the tuple belongs to the S relation, then we have to store three
                //values, the origin, the S.a and the S.x
                outputValue = new Text(tuple.get(0) + "," + tuple.get(1) + "," + tuple.get(2));
            }
        } else { //R relation
            //As with the S-tuple, if the aforementioned R-tuple fails
            //to pass the filter, do not output it to the reducer
            if (Integer.parseInt(tuple.get(1)) <= 10) {
                filterFail = true;
            } else {
                //If the tuple belongs to the R relation, we have to only store two
                //values, the origin and S.a
                outputValue = new Text(tuple.get(0) + "," + tuple.get(1));
            }
        }
        //If the tuple passes the filter then and only *then* output it to the reducer.
        //The boolean flag is already defined as 'false' (the tuple passes the filter)
        //so if it will pass the filter, it will normally be output to the reducer
        if (!filterFail) {
            //Iteration to output regionIDs.size() copies of the tuple
            //to regionIDs.size() reducers. The key is set to the
            //specific regionID in every iteration
            for (int i = 0; i < regionIDs.size(); i++) {
                outputKey = new Text(String.valueOf(regionIDs.get(i)));
                context.write(outputKey, outputValue);
            }
        }
    }
}
