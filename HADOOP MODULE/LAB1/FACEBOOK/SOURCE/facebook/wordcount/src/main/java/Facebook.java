import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.*;


public class Facebook {

    //Mapper Class
    public static class FMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] ip = value.toString().split("-"); //Splitting the input "-"
            if (ip.length == 2) {
                String frend1 = ip[0]; //This line is written to get the usernames of people
                List<String> values = Arrays.asList(ip[1].split(",")); //Splitting of friends based on ','
                for (String friend2 : values)
                {
                    //Getting each friend's value and grouping

                    if (Integer.parseInt(frend1) < Integer.parseInt(friend2))
                        word.set(frend1 + "," + friend2); //Setting word as mapping output.
                    else
                        word.set(friend2 + "," + frend1);
                    context.write(word, new Text(ip[1]));
                }
            }
        }

    }
    public static class FReducer extends Reducer<Text, Text, Text, Text>
    {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            HashMap<String, Integer> FMap = new HashMap<String, Integer>();
            StringBuilder sub = new StringBuilder();
            for (Text friends : values)
            {
                List<String> temporary = Arrays.asList(friends.toString().split(","));
                for (String mutual : temporary)
                {
                    if (FMap.containsKey(mutual))
                        sub.append(mutual + ',');
                    else
                        FMap.put(mutual, 1);

                }
            }

            result.set(new Text(sub.toString()));
            context.write(key, result); //Generating the reduced output.
        }
    }


    //This is a driver Class
    public static void main(String[] args) throws Exception
    {
        if (args.length != 2)
        {
            System.err.println("ERROR! INSUFFICIENT NUMBER OF ARGUMENTS"); //If the number of program arguments passed are less than two, Print this statement.
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = new Job(conf, "MutualFriends");
        job.setJarByClass(Facebook.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(FMapper.class);
        job.setReducerClass(FReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); //Input file
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //Output file

        job.waitForCompletion(true);

    }


}