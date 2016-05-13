/*
 * Utilities class that has all the secondary methods
 * for string split, random numbers calculations etc
 * */
package com.lefteris008.hadooptheta.utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 *
 * @author Lefteris Paraskevas
 * @author Alexandros Tzanakas
 * @version 2016.05.14_0029
 */
public class Utilities {

    private final static char SEPARATOR = ',';

    /**
     * Calculate a random integer between min and max.
     *
     * @param min Lower bound
     * @param max Upper bound
     * @return An random integer [min, max]
     */
    public final static int randInt(int min, int max) {
        Random rand = new Random();

        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int randomNum = rand.nextInt((max - min) + 1) + min;

        return randomNum;
    }

    /**
     * Method to read and extract a tuple from a String (a line from the input
     * file) which finally stores it in an ArrayList and returns it
     * @param line A String containing a line from file
     * @return A List containing the extracted tuple
     */
    public static List<String> extractTupleFromLine(String line) {
        //Initialize ArrayList which will be returned to user
        //as also as character array temp that will contain
        //the tuple as it is extracted from the file
        List<String> tuple = new ArrayList<>();
        char[] charArray = new char[line.length()];
        charArray = line.toCharArray();

        List<Character> temp = new ArrayList<>();

        //Add the relation's name
        tuple.add(String.valueOf(charArray[0]));

        int i = 1;
        while (i < charArray.length) {
            //If charArray[i] is ',' then proceed to the next character
            if (charArray[i] == SEPARATOR) {
                i++;
            } else {
                while (charArray[i] != SEPARATOR) {
                    temp.add(charArray[i]);
                    i++;
                    if (i == charArray.length) {
                        break; 	//Case where i exceeds charArray.length
                        //while searching for characters
                    }
                }
                tuple.add(getStringRepresentationFromCharArray(temp));
                temp.clear();
                i++;

            }
        }
        return tuple;
    }

    /**
     * Generates a String for the give Character array.
     * @param list A list containing characters
     * @return The String representation of the input
     */
    public static String getStringRepresentationFromCharArray(List<Character> list) {
        StringBuilder builder = new StringBuilder(list.size());
        for (Character ch : list) {
            builder.append(ch);
        }
        return builder.toString();
    }
}
