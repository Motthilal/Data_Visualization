
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class equijoin
{
  public static class Map extends Mapper<Object,Text,Text,Text>
  {
    private Text ekey=new Text();
    private Text evalue=new Text();

    public void map (Object key, Text value, Context context) throws IOException, InterruptedException
    {
      String record=value.toString();
      String[] col = record.split(",");
      String eachcol=col[1];
      ekey.set(eachcol);
      evalue.set(record);
      context.write(ekey,evalue);

    }

  }

  public static class Reduce extends Reducer<Text, Text, Text, Text>
  {
  
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
    {
      
        List<String> Table1 = new ArrayList<String>();
        List<String> Table2 = new ArrayList<String>();
        List<String> Wobj = new ArrayList<String>();

        Text rvalue=new Text();
        Text rkey = new Text("");
        String rmidval=new String();
        boolean flag_check=true;
        String tempname="";

        for (Text each : values)
        {
          String record = each.toString();
          String[] col=record.split(",");
          if(flag_check == true){
              tempname=col[0];
            flag_check=false;
          }

          if(tempname==col[0]){
            Table1.add(record);
          }
          else{
            Table2.add(record);
          }
          Wobj.add(record);
        }

        Collections.reverse(Wobj);

        if(Table1.size()==0 || Table2.size()==0){
          key.clear();
        }
        else{
          for( int row=0;row<Wobj.size();row++){
            for(int column=row+1; column<Wobj.size();column++){
              if(!Wobj.get(row).split(",")[0].equalsIgnoreCase(Wobj.get(column).split(",")[0])){
                  rmidval=Wobj.get(row)+", "+Wobj.get(column);
                  rvalue.set(rmidval);
                  context.write(rkey,rvalue);
 
              }
              }
          }
      }



  }


} 
  public static void main(String[] args) throws Exception
  {
       Configuration config = new Configuration();
       Job joinjob = Job.getInstance(config, "equijoin");
       joinjob.setJarByClass(equijoin.class);
       joinjob.setMapperClass(Map.class);
       joinjob.setReducerClass(Reduce.class);
       joinjob.setOutputKeyClass(Text.class);
       joinjob.setOutputValueClass(Text.class);
       FileInputFormat.addInputPath(joinjob, new Path(args[0]));
       FileOutputFormat.setOutputPath(joinjob, new Path(args[1]));
       System.exit(joinjob.waitForCompletion(true) ? 0 : 1);

  }
}