package com.eswalker.msd.PostingsListGenerator;
import java.util.ArrayList;
/**
 * Mimics the functionality of Java's string.split(String regex)
 * class but with a single character instead of regex.  
 * 
 * Tested 58% faster on comma separated audience files than Java's
 * implementation.
 * 
 * @author Ed Walker
 */
public class Split {
	
	/**
     * Splits line by tabs. Within quotes is escaped
     * @param line the line you want to split
     * @return an array of String tokens
     */
	public static String[] tabSplit(String line) {
    	return charSplit(line, '\t');
    }
	 
	/**
     * Splits line by comma. Within quotes is escaped
     * @param line the line you want to split
     * @return an array of String tokens
     */
    public static String[] commaSplit(String line) {
    	return charSplit(line, ',');
    }
    
    /**
     * Splits line by character. Within quotes is escaped
     * @param line the line you want to split
     * @param c the character you want to split by
     * @return an array of String tokens
     */
    public static String[] charSplit(String line, char c) {
    	
    	
        
        boolean inQuote = false;
        ArrayList<Integer> splits = new ArrayList<Integer>();
        for (int j = 0; j < line.length(); j++) {
        	 if (inQuote && line.charAt(j) == '"') {
        		inQuote = false;
        	} else if (!inQuote && line.charAt(j) == '"') {
        		inQuote = true;
        	} else if (!inQuote && line.charAt(j) == c) {
        		splits.add(j);
        	}
        }
    	String[] arr = new String[splits.size() + 1];
    	
    	if (splits.isEmpty()) {
    		arr[0] = line;
    		return arr;
    	}
    	
    	arr[0] = line.substring(0, splits.get(0));
    	for (int i = 1; i < splits.size() ; i++) {
    		arr[i] = line.substring(splits.get(i-1) + 1, splits.get(i));
    	}
    	arr[splits.size()] = line.substring(splits.get(splits.size() - 1) + 1);
    	return arr;

    }
}
