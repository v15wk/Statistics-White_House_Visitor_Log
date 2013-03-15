PROBLEM STATEMENT:

Please apply Hadoop mapreduce to derive some statistics from White House Visitor Log. There are currently 2.9 million records available at

http://www.whitehouse.gov/briefing-room/disclosures/visitor-records

Data is available as web only spreadsheet view and downloadable raw format in CSV (Comma Separated Value). In CSV format each column is separated by a comma “,” in each line. The first line represents the heading for the corresponding columns in other lines. We are going to use this raw data for our mapreduce operation.

You are required to write efficient Hadoop MapReduce programs in Java to find the following information:

(i) The 10 most frequent visitors (NAMELAST, NAMEFIRST) to the White House.

(ii) The 10 most frequently visited people (visitee_namelast, visitee_namefirst) in the White House.

(iii) The 10 most frequent visitor-visitee combinations.

(iv) Average number of visitors by month of a year. Say what is the average number of visitors in January over last 4 years. Your program should produce average number of visitors for 12 months. Consider APPT_START_DATE as the visit date value.

Consider mapreduce chaining for efficiency.


STEPS TO HADOOP-COMPILE AND RUN MY CODE:

STEP 1. Place respective java file in Unix[Assignment1.java/Assignment2.java/Assignment3.java/Assignment4.java]

STEP 2. Clear Output directories to remove any previous output

STEP 3. Place Input files in Input directory

STEP 4. Compile the java code using following command:
javac -classpath /usr/local/hadoop-1.0.4/hadoop-core-1.0.4.jar -d assignment3_classes assignment3.java

STEP 5. Create the jar using following command:
jar -cvf assignment3.jar -C assignment3_classes/ .
		
STEP 6. Run the jar file using following command:
hadoop jar assignment3.jar assignment3 ./input ./tmp ./output [NOTE: For Assignment4, there are only two arguments INPUT and OUTPUT]

STEP 7. Read the output using following command:
hadoop fs -cat ./output/part-r-00000