import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.io.*;

public class OneStopFlightMapper extends Mapper<LongWritable, Text, Text, OneStopTuplePair> {
  // Indices of join attributes for both relations
  //public static final int REL_A_JOINKEY_IND = 3;
  //public static final int REL_B_JOINKEY_IND = 2;
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    OneStopTuplePair outValue = new OneStopTuplePair();
    int joinIndex;
    
    //retrieving the filename for which the input record comes from: orders.txt or customers.txt
    String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
    
    
    // initializing outevalue.relationName and joinIndex
    if (fileName.equals("sample1.csv")) {
      outValue.setRelationName("A");
     // joinIndex = REL_A_JOINKEY_IND;
    } else {
      outValue.setRelationName("B");
      //joinIndex = REL_B_JOINKEY_IND;
    }
    // outkey= a comma-separated list of values of the join attributes
    // outTuple= a comma-separated lust of values of non-join attributes
    String[] records = line.split(",");
    if(this.isNumeric(records[0])){
    	String origin = records[16];
    	String destination = records[17];
    	int index = 0;
        String outKey = "", outTuple = "";

        if(outValue.getRelationName().equalsIgnoreCase("A")){
        	outKey += destination;
        }else{
        	outKey += origin;
        }
        //year, month, day, departure time, scheduled arrival time, unique carrier, 
        //flight number, origin, destination
        outTuple = records[0]+","+records[1]+","+records[2]+","+records[5]+","+records[7]
        		+","+records[8]+","+records[9]+","+records[16]+","+records[17];
        
        outKey += records[0]+","+records[1]+","+records[2]+","+records[8];
        
        outValue.setTuple(outTuple);
        
        context.write(new Text(outKey), outValue);
    }
    
  }
  
  public boolean isNumeric(String value){
	  try{
		  Integer.parseInt(value);
		  return true;
	  }catch(NumberFormatException exp){
		  return false;
	  }
  }
}
