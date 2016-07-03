import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class OneStopFlightReducer extends Reducer<Text, OneStopTuplePair, NullWritable, Text> {

  public void reduce(Text joinfield, Iterable<OneStopTuplePair> values, Context context)
      throws IOException, InterruptedException {

    //An array to store the tuples coming from the customer relation
	  ArrayList<String> CRecords = new ArrayList<String>();
	  
	//an array to store the tuples coming from the orders relation
	  ArrayList<String> ORecords = new ArrayList<String>();
	 
   
    //Separating the tuples associated with customers and orders into CRecords and ORecords
    for (OneStopTuplePair tp : values) {

      if (tp.getRelationName().equals("A"))
        CRecords.add(tp.getTuple());
      else
        ORecords.add(tp.getTuple());

    }
   //Emitting all the combinations  of the tuples in CRecords and ORecords
    String outValue;
    for (String record1 : CRecords)
      for (String record2 : ORecords) {
        //outValue = joinfield + "," + record1.toString() + "," + record2.toString();
        String[] relAValues = record1.split(",");
        String[] relBValues = record2.split(",");
        String aDateString = relAValues[0]+"/"+relAValues[1]+"/"+relAValues[2]; 
        String bDateString = relBValues[0]+"/"+relBValues[1]+"/"+relBValues[2];
        //if(relAValues[5].equalsIgnoreCase(relBValues[5]) && aDateString.equalsIgnoreCase(bDateString)
        //	&& relAValues[6].equalsIgnoreCase(relBValues[7])){
        	String bDepartTime = relBValues[3];
        	String aArrivalTime = relAValues[4];
        	
        	double diff = getDifferenceHours(bDateString, bDepartTime, aDateString, aArrivalTime);
        	if(diff >=1 && diff <= 5){
        		String resultString = relAValues[6]+"	"
        				+relBValues[6]+"	"+relAValues[7]+"	"+relAValues[8]+"	"
        				+relBValues[7]+"	"+relBValues[8]+"	"+aArrivalTime+"	"+bDepartTime;
        		context.write(NullWritable.get(), new Text(resultString));
        	}
        	
        //}
        
      }
  }
  
  public double getDifferenceHours(String bDateString, String bDepartTime, String aDateString, String aArrivalTime){
	  DateFormat df = new SimpleDateFormat("yyyy/MM/dd HHmm");
	  if(bDepartTime != null && bDepartTime.length() < 4){
		  int diff = 4 - bDepartTime.length();
		  String zeroPrefix = "";
		  for(int i=0;i<diff;i++){
			  zeroPrefix += "0"; 
		  }
		  bDepartTime = zeroPrefix + bDepartTime;
	  }else if(bDepartTime == null){
		  bDepartTime = "0000";
	  }
	  if(aArrivalTime != null && aArrivalTime.length() < 4){
		  int diff = 4 - aArrivalTime.length();
		  String zeroPrefix = "";
		  for(int i=0;i<diff;i++){
			  zeroPrefix += "0"; 
		  }
		  aArrivalTime = zeroPrefix + aArrivalTime;
	  }else if(aArrivalTime == null){
		  aArrivalTime = "0000";
	  }
	  Date d1 = null, d2 = null;
	  try {
			d1 = df.parse(bDateString+" "+bDepartTime);
			d2 = df.parse(aDateString+" "+aArrivalTime);
			long diff = d1.getTime() - d2.getTime();
			
			double diffSeconds = diff / 1000 % 60;
			double diffMinutes = diff / (60 * 1000) % 60;
			double diffHours = diff / (60 * 60 * 1000) % 24;
			diffHours = diffHours + (diffMinutes / 60) + (diffSeconds / 3600);
			
			return diffHours;
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  return 0;
  }
}
