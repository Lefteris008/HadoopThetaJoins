package com.lefteris008.hadooptheta.thetaphase;

import com.lefteris008.hadooptheta.utilities.Utilities;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Lefteris Paraskevas
 * @author Alexandros Tzanakas
 * @version 2016.05.14_0038
 */
public class ThetaJoinReduce extends Reducer<Text, Text, Text, Text> {

    /**
     * This Reducer gets a single key and a list of values which are R-tuples, 
     * S-tuples or both. It then divides them in two separate data structures, 
     * one for the R-tuples and one for the S-tuples. After the division, it 
     * joins them. More specifically, it compares every R-tuple with all the
     * S-tuples in the tuplesFromS TreeMap and if a R.a attribute is lower than 
     * a S.a attribute, it sums the S.x attribute of the S-tuple. At the end, it 
     * outputs the R.a attribute and the summary of the S.x attribute as the value 
     * (the key is set to 'null'). Because the algorithm of the Mapper can (and has 
     * to) pass more than one copies of a single R-tuple to more than one reducers, 
     * this local summary of the S.x attribute doesn't represent the full summary, 
     * but only a partial one (semi-summary) that will be output from the reducer 
     * to the file and it will be used to the second Map/Reduce phase.
     * [By creation, a specific R-tuple cannot be joined with a specific S-tuple 
     * more than once in *all* the reducers. So, the summary that is calculated 
     * to a single reducer instance for a specific R-tuple, is different from 
     * another reducer instance for the same R-tuple; the summary of these 
     * semi-summaries, gives us the full summary of all the S.x attributes of 
     * all the S-tuples that a specific R-tuple is going to be joined]
     * 
     * ==Optimization==
     * We optimized the execution by creating a TreeMap that contains the S.a 
     * attribute as the key and all the corresponding S.x attributes of this S.a, 
     * as values. We used the TreeMap structure because it is sorted. Now, every 
     * R.a attribute is compared with the KeySet of this TreeMap which contains 
     * the grouped S.a attributes. Because this KeySet is sorted by descending 
     * order, when we find a S.a attribute which is lower than or equal the R.a 
     * attribute, we break the iteration because all of the other S.a attributes 
     * in this KeySet are going to be greater than or equal also. Note that this 
     * optimization lowers the final number of comparisons for a large number of 
     * S-tuples only (because the number of the S-tuples is large, the possibility 
     * of more than one S-tuples having the same S.a attribute is high, so we 
     * group them and we are only comparing them with a single R.a attribute
     * once), while for "normal" numbers it may has the same number of comparisons 
     * with the no-TreeMap implementation (comparing every R.a attribute with 
     * every S.a attribute)
     * 
     * ==Note==
     * BigInteger is used to support large datasets (in terms of GBs) that will 
     * normally exceed the limit of the int type.
     * @param key A Text key for the reducing process
     * @param values An Iterable of Text values for the reducing process
     * @param context
     * @throws IOException
     * @throws InterruptedException 
     */
    public final void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {

        //Initialize data structures for the tuples of relation R and S
        List<String> tuplesFromR = new ArrayList<>();
        Map<Integer, ArrayList<String>> tuplesFromS_Hash = new TreeMap<>(Collections.reverseOrder());

        //Secondary data structures
        String testTuple = null;
        String[] testTupleArray = null;

        //Iterate through the input tuples
        while (values.iterator().hasNext()) {

            //For every tuple
            testTuple = values.iterator().next().toString();
            testTupleArray = testTuple.split(",");
            //Check if it's from the 'R' or from the 'S' relation
            //and store it to the specific data structure
            if (testTuple.charAt(0) == 'S') {
                //Check if the tuplesFromS_Hash has already stored values for
                //a specific S.a attribute key and either initialize it or add the
                //new S.x attribute as value
                if (tuplesFromS_Hash.containsKey(Integer.parseInt(testTupleArray[1]))) {
                    tuplesFromS_Hash.get(Integer.parseInt(testTupleArray[1])).add(testTupleArray[2]);
                } else {
                    tuplesFromS_Hash.put(Integer.parseInt(testTupleArray[1]), 
                            new ArrayList<String>(Arrays.asList(testTupleArray[2])));
                }
            } else { //R relation
                tuplesFromR.add(testTuple);
            }
        }

        //Calculate the join result
        List<String> tupleFromR = new ArrayList<>();
        List<String> tupleFromS = new ArrayList<>();

        BigInteger countX = new BigInteger("0");
        Text outputValue = null;
        Iterator<Integer> it = null;
        for (int i = 0; i < tuplesFromR.size(); i++) {
            //For every single R-tuple in tuplesFromR
            tupleFromR = Utilities.extractTupleFromLine(tuplesFromR.get(i));

            it = tuplesFromS_Hash.keySet().iterator();
            int hash_key;
            int j;
            BigInteger tempSum;
            //Compare it with all the S-tuples in tuplesFromS_Hash TreeMap
            while (it.hasNext()) {
                hash_key = (int) it.next(); //Store the key (S.a attribute)

                //Get the S.x attributes of this S.a key
                //(The ArrayList will contain either a single or more than one
                //S.x attributes, regarding the number of the S-tuples)
                tupleFromS = new ArrayList<>(tuplesFromS_Hash.get(hash_key));

                // Join condition (note that the filter is already applied in the Mapper)
                if (Integer.parseInt(tupleFromR.get(1)) < hash_key) {
                    //Calculate the sum of the S.X attribute which will be
                    //used to the second Map/Reduce phase
                    for (j = 0; j < tupleFromS.size(); j++) {
                        tempSum = new BigInteger(tupleFromS.get(j));
                        countX = countX.add(tempSum);
                    }

                } else { //R.a is greater than or equal a single S.a attribute
                    //Break the iteration, because all the remaining
                    //S.a attributes in the KeySet, will be lower than R.a
                    //(The KeySet is sorted in descenting order)
                    break;
                }
            }
            outputValue = new Text(tupleFromR.get(1) + "," + String.valueOf(countX));
            //Output only R.a attribute and the S.x semi-summary
            context.write(null, outputValue);
            countX = new BigInteger("0");
        }
    }
}
