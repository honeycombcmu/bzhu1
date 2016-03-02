
import java.io.IOException;
package com.mycompany.app;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.*

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RandomForestDriver {

    public class RandomForestMapper extends Mapper<LongWritable, Text, Text, Text> {

    DataFrame trainDataWhole;
    DataFrame trainData;
    DataFrame testData;


    @Override
    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      
        StringTokenizer itr = new StringTokenizer(ivalue.toString());
        //while (itr.hasMoreTokens()) {
        //String[] tokens = itr.nextToken().split(",");
        //if (tokens != null) {
        //}
        //}
        ArrayList<String> result = new ArrayList<>();
        trainData = new DataFrame();
        for (int i = 0; i < 100; i++) {
          int size = trainDataWhole.getEntityCount();
          Random random = new Random();
          int index = random.nextInt(size);
          trainData.append(trainDataWhole.getEntity(index));
        }
        DecisionTreeNode node = new DecisionTreeNode(trainData);
        node.train(trainData);
        result = node.test(testData);
        for (int i = 0; i < result.size(); i++) {
             key = i;
             value = new Text(result.get(i));
             context.write(key, value);
        }

    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
      Configuration conf = context.getConfiguration();
      Path ptrain = new Path(conf.get("traindatafile"));
      trainDataWhole = new DataFrame(ptrain);
      Path ptest = new Path(conf.get("testdatafile"));
      testData = new DataFrame(ptest);
      //FileSystem fs = FileSystem.get(new Configuration());
      //BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
    }

	/*protected void cleanup(Context context) throws IOException, InterruptedException {
		
    	int sampleSize = samples.size();
    	
    	List<Entity> trainData = new ArrayList<>();
        
        for(int i = 0; i < sampleSize; i++) {
  	      int randInt = MathUtils.randomInt(0, sampleSize);
  	      DataSample ds = samples.get(randInt);
  	      trainData.add(ds);
        }
    
        DecisionTree tree = new RandomTree(numFeatures);
        List<List<Feature>> features = getFeatures(trainData);
        tree.train(trainData, features);
    	
        context.write(new Text("Tree"), new Text(tree.JSONTree()));
    }*/
}

public class RandomForestReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<Text, Integer> freqMap = new HashMap<String, Integer>();
        	for(Text v: values) {
        		Integer frequency = freqMap.get(v);
        		if (frequency == null) {
        			freqMap.put(v, 1);
        		} else {
        			freqMap.put(v, frequency + 1);
        		}
        	}
        	Text mostCommonLable = null;
        	int maxFrequency = -1;
        	for (Map.Entry<Text, Integer> entry : freqMap.entrySet()) {
        		if (entry.getValue() > maxFrequency) {
        			mostCommonLable = entry.getKey();
        			maxFrequency = entry.getValue();
        		}
        	}
        	context.write(key, mostCommonLable);
        }
}

    public static void main(String[] args) throws Exception
	   {
		// Create configuration
		Configuration conf = new Configuration();
		conf.set("traindatafile", args[2]);
		conf.set("testdatafile", args[3]);
		// Create job
		Job job = Job.getInstance(conf, "Random Forest");
		job.setJarByClass(RandomForestDriver.class);	
		// Setup MapReduce job
		job.setMapperClass(RandomForestMapper.class);
		job.setReducerClass(RandomForestReducer.class);
		//job.setNumReduceTasks(1); // Only one reducer in this design

		// Specify key / value
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
				
		// Input (the data file) and Output (the resulting classification)
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// Execute job and return status
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}





