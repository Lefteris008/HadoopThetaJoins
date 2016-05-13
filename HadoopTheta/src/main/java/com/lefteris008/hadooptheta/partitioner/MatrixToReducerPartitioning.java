package com.lefteris008.hadooptheta.partitioner;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Lefteris Paraskevas
 * @authot Alexandros Tzanakas
 * @version 2016.05.14_0001
 */
public class MatrixToReducerPartitioning implements Serializable {

    private static volatile MatrixToReducerPartitioning object = null;
    private static int S; //Size of S
    private static int R; //Size of R
    public static int r;  //Number of reducers
    
    //Intersecting reducers in current row and column
    private final static Map<Integer, ArrayList<Integer>> interRows = new HashMap<>();
    private final static Map<Integer, ArrayList<Integer>> interColumns = new HashMap<>();
    
    /**
     * Empty constructor
     */
    public MatrixToReducerPartitioning() {
        ///
    }

    /**
     * Parametrized constructor.
     * @param inputS Relation S
     * @param inputR Relation R
     * @param input_r Number of reducers 
     */
    public MatrixToReducerPartitioning(int inputS, int inputR, int input_r) {
        S = inputS;
        R = inputR;
        r = input_r;
    }

    /**
     * Thread lock for perceiving a MatrixToReducerPartitioning object in memory.
     * @return A MatrixToReducerPartitioning object.
     */
    public final static MatrixToReducerPartitioning getObject() {
        if (object == null) {
            synchronized (MatrixToReducerPartitioning.class) {
                if (object == null) {
                    object = new MatrixToReducerPartitioning();
                }
            }
        }
        return object;
    }

    /**
     * Return the cardinality of the S relation.
     * @return An integer representing the cardinality of S.
     */
    public final int getSCardinality() {
        return S;
    }

    /**
     * Return the cardinality of the R relation.
     * @return An integer representing the cardinality of R.
     */
    public final int getRCardinality() {
        return R;
    }

    /**
     * Return an ArrayList containing the regions in a specific row.
     * @param row An integer pointing to a row
     * @return An Integer list containing all rows that intersect with 'row'
     */
    public final ArrayList<Integer> getIntersectingRows(int row) {
        return interRows.get(row - 1);
    }

    /**
     * Return an ArrayList containing the regions in a specific column.
     * @param column An integer pointing to a column
     * @return An Integer list containing all columns that intersect with 'column'
     */
    public final ArrayList<Integer> getIntersectingColumns(int column) {
        return interColumns.get(column - 1);
    }

    /**
     * With this method we partition a matrix of size S * R into r blocks. 
     * We take the square root of the r, which gives us an approximate distribution of 
     * the reducers in rows and columns. Then we take the ceiling and the bottom 
     * so that we have integers for each side. If the number of reducers is bigger 
     * than that of the multiples of the square root (e.g 43 gives us two square 
     * roots of 7 * 6 = 42) we partition the matrix for the 42 reducers, recalculate 
     * the size that these 42 reducers need and then we add the remaining reducer 
     * at the end. This is possible with the optimal square area. Because we
     * have the approximate area for each reducer, we can then calculate the 
     * remaining side (e.g where the 42 reducers end and where the 43rd reducer 
     * should be put).
     */
    public final void Partitioner() {
        if (S * R < r) {
            System.out.println("ERROR: The number of reducers is greater than "
                    + "the cells of the matrix!");
            System.exit(0);
        } else if (r <= 0) {
            System.out.println("ERROR: The number of reducers is not valid!");
            System.exit(0);
        }

        //Arrays where we will put the position of the reducers in columns and ]
        //in rows
        List<Integer> arrayCol = new ArrayList<>(); 
        List<Integer> arrayRow = new ArrayList<>();

        int reducer = 1;// The number of the reducer we are about to save
        int newReducers = r;// The number of all reducers
        int newS = S;// The size of S
        double optSqrArea = Math.round((double) S * R / r); // Optimal square area for each block
        double cs = Math.ceil(Math.sqrt(newReducers));// How many reducers in rows
        double ct = Math.floor(Math.sqrt(newReducers));// How many reducers in columns
        double d = Math.sqrt(newS * R / r);// Optimal side size
        double es = newS - d * cs;// Calculate remaining length;
        double et = R - d * ct;// Calculate remaining width;
        double rowSide = Math.floor(d + es / cs);// Calculate the length of each block
        double colSide = Math.floor(d + et / ct);// Calculate the width of each block
        boolean flag = false;// Auxiliary variable
        int remainingReducers = 0;// Remaining number of reducers to be processed

        if (R < S / r) { //If the matrix is row-shaped
            arrayRow.add(0); //We add the first row
            for (int i = 1; i <= r; i++) { //We break the matrix into partitions
                arrayRow.add(i * S / r); //Add the points into an array
            }
        } else {// For any other shape
            if (ct > R) { //If the number of the reducers in width are more than the R (columns)
                ct = R; //Reducers in column become as many as the columns
                cs = Math.floor(r / ct); //The remaining reducers are distributed in rows
            }

            if (cs * ct < r) {	//If the number of the square root is smaller than that of the user input
                optSqrArea = Math.round((double) S * R / r);
                newReducers = (int) (cs * ct); //New reducers are as many as the product of the factors of the square root
                remainingReducers = r - newReducers; //Reducers that are left outside the current matrix
                newS = (int) (S - Math.ceil(optSqrArea * remainingReducers / R)); //Calculate new row side
                rowSide = Math.floor(newS / cs); //Calculate the size of each block in rows
                colSide = Math.floor(R / ct); //Calculate the size of each block in columns
                flag = true;
            } else if (cs * ct > r) { //If the number of the square root is greater than that of the user
                cs = Math.floor(Math.sqrt(newReducers)); //Take the floor of the square for this side instead of ceiling
                newReducers = (int) (cs * ct); //New number of reducers
                optSqrArea = Math.floor((double) newS * R / r); //Find the new optimal square area
                newS = (int) Math.floor(newReducers * optSqrArea / R); //Calculate the size of S in rows
                rowSide = Math.floor(newS / cs); //Calculate the size of each block in rows
                colSide = Math.floor(R / ct); //Calculate the size of each block in columns
                remainingReducers = r - newReducers; //Reducers that are left outside current matrix
                flag = true;
            }

            if (r == 3) { //Special case if the reducers are 3
                cs = 1; //We only need 1 reducer in rows
            }

            if (colSide == 0) { //If the column size of each block is 0, it means that we have many reducers and rounding to bottom gives 0
                colSide = 1; //Column size becomes 1
                ct = R; //Reducers in column become as many as R
                cs = r / ct; //How many reducers in rows
                if (r <= R * S) { //If the reducers are not as many as the area
                    newS = S;
                    rowSide = Math.floor(newS / cs);
                }
            }

            double rowLength = 0;
            for (int i = 0; i < cs; i++) { //Find where each block ends
                if ((newS % rowSide != 0) && (flag)) {
                    //If we have entered one of the previous 2 if statements we
                    //have to compensate for the loss of digits because of the
                    //rounding to the bottom
                    if (i % 2 == 0) { //So after 2 repeats we add 1 to each block size
                        arrayRow.add((int) (rowLength));
                        rowLength = rowLength + rowSide;
                    } else {
                        arrayRow.add((int) (rowLength));
                        rowLength = rowLength + rowSide + 1;
                    }
                } else {
                    //If we have not entered one of the previous 2 statements we simply add the
                    //size each block
                    arrayRow.add((int) (i * rowSide));
                }
            }
            for (int i = 0; i < ct; i++) {
                // We add were each side of the block in columns ends
                arrayCol.add((int) (i * colSide));
            }
            if (r == 3) { //Special case if the reducers are 3
                for (int i = 1; i <= Math.floor(Math.sqrt(newReducers)); i++) {
                    arrayCol.add((int) (i * (colSide / 2)));
                }
            }
            if ((arrayCol.get(arrayCol.size() - 1)) < R) {
                //We add the ending point of the columns to the array
                arrayCol.add(R);
            }
            if (arrayRow.get(arrayRow.size() - 1) < newS) {
                //We add the ending point of the columns to the array
                arrayRow.add(newS);
            }

            int i, j = 0;
            for (i = 0; i < arrayRow.size() - 1; i++) {
                //We save each reducer in the blocks we calculated before
                for (j = 0; j < arrayCol.size() - 1; j++) {
                    intersectingRows(arrayRow.get(i), arrayRow.get(i + 1), reducer);
                    intersectingColumns(arrayCol.get(j), arrayCol.get(j + 1), reducer);
                    reducer++; //Increment the reducer
                }
            }

            if (flag == true) {
                //If the auxiliary flag was changed we need to add the remaining
                //reducers at the end of the matrix
                colSide = Math.ceil(R / remainingReducers); //Calculate how many columns each block will need
                arrayRow.clear(); //Empty the arrays
                arrayCol.clear();

                for (int k = 0; k < remainingReducers; k++) {
                    arrayCol.add((int) (k * colSide)); //Add where each block ends in columns
                }
                if ((arrayCol.get(arrayCol.size() - 1)) < R) {
                    arrayCol.add(R); //Add the last column
                }
                arrayRow.add(newS); //Add the beginning row for each block
                arrayRow.add(S); //Add the ending row for each block

                for (i = 0; i < arrayRow.size() - 1; i++) {
                    //Save the reducers in each block
                    for (j = 0; j < arrayCol.size() - 1; j++) {
                        intersectingRows(arrayRow.get(i), arrayRow.get(i + 1), reducer);
                        intersectingColumns(arrayCol.get(j), arrayCol.get(j + 1), reducer);
                        reducer++;
                    }
                }
            }
        }
    }
    
    /**
     * Method to store all intersecting reducers in a row.
     * @param rowNow Integer indicating the current row.
     * @param rowNext Integer indicating the next available row.
     * @param reducer Integer indicating the current reducer.
     */
    private final void intersectingRows(int rowNow, int rowNext, int reducer) {
        for (int i = rowNow; i < rowNext; i++) {
            if (interRows.containsKey(i)) { //If we already have the row saved
                if (!interRows.get(i).contains(reducer)) {
                    interRows.get(i).add(reducer); //We add the current reducer
                }
            } else { //Else create the row
                interRows.put(i, new ArrayList<Integer>(Arrays.asList(reducer))); 
            }
        }
    }

    /**
     * Method to store all intersecting reducers in a column.
     * @param colNow Integer indicating the current column.
     * @param colNext Integer indicating the next available column.
     * @param reducer Integer indicating the current reducer.
     */
    private final void intersectingColumns(int colNow, int colNext, int reducer) {
        for (int i = colNow; i < colNext; i++) {
            if (interColumns.containsKey(i)) { //If we have the column already saved
                if (!interColumns.get(i).contains(reducer)) {
                    interColumns.get(i).add(reducer); //Add the reducer
                }
            } else { //Else create the column
                interColumns.put(i, new ArrayList<Integer>(Arrays.asList(reducer))); 
            }
        }
    }
}
