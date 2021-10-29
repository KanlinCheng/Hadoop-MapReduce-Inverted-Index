import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexJob {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

    // Text is a hadoop specific datatype that is used to handle Strings in a hadoop environment instead of Java's String datatype.
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      // Split the docID and the text content.
      String docId = value.toString().substring(0, value.toString().indexOf("\t"));
      String valueOriginal =  value.toString().substring(value.toString().indexOf("\t") + 1);
      // Convert all non-alphabetical characters to space, and transfer all the characters to lower case.
      String valueOnlyAlpha = valueOriginal.replaceAll("[^a-zA-Z]", " ").toLowerCase();
      
      // Set an iterator to read input one line at a time and tokenize by space.
      StringTokenizer itr = new StringTokenizer(valueOnlyAlpha, " ");
      
      // Iterate through all the words and generate the [word docID] key-value pairs.
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        if(!word.toString().isEmpty() && word.toString() != ""){
          context.write(word, new Text(docId));
        }
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
    
    // This Reducer class is to collect output of the Mapper and aggregate the docID where the word appears,
    // and calculate the sum of appearance along with the docID.

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      HashMap<String,Integer> docHashMap = new HashMap<String,Integer>();
      
      for (Text val : values) {
        if (docHashMap.containsKey(val.toString())) {
          docHashMap.put(val.toString(), docHashMap.get(val.toString()) + 1);
        } else {
          docHashMap.put(val.toString(), 1);
        }
      }
      StringBuilder docCountSummary = new StringBuilder();
      for(String docID : docHashMap.keySet()){
        docCountSummary.append(docID + ":" + docHashMap.get(docID) + " ");
      }
      context.write(key, new Text(docCountSummary.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Inverted index");
    job.setJarByClass(InvertedIndexJob.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
