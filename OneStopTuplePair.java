import java.io.*;

import org.apache.hadoop.io.*;

//A custom value class to hold a tuple and the name of the relation that contains it.
public class OneStopTuplePair implements Writable {
  // A comma seperated tuple
  private Text tuple;
  // The name of the relation that contains the tuple
  private Text relationName;
  
  // Default Constructor
  public OneStopTuplePair() {
    tuple = new Text();
    relationName = new Text();
  }
  // constructor initializing the data fields
  public OneStopTuplePair(String relationName, String tuple) {
    this.relationName = new Text(relationName);
    this.tuple = new Text(tuple);
  }
  // Implementing the methods of Writable interface
  public void readFields(DataInput in) throws IOException {
    tuple.readFields(in);
    relationName.readFields(in);
  }
  public void write(DataOutput out) throws IOException {
    tuple.write(out);
    relationName.write(out);
  }
  // Accessors and mutators
  public String getRelationName() {
    return this.relationName.toString();
  }

  public String getTuple() {
    return this.tuple.toString();
  }

  public void setRelationName(String name) {
    this.relationName = new Text(name);
  }

  public void setTuple(String t) {
    this.tuple = new Text(t);
  }
  public String toString()
  {
	  return this.relationName.toString() + " "+ this.tuple.toString();
  }
}
