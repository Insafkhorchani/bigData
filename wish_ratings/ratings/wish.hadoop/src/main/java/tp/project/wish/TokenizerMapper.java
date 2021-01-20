package tp.project.wish;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;



public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    private  static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(),",");

        String[] l = value.toString().split(",");
        Integer promo = Integer.valueOf(l[3].toString());

        word.set( l[0].toString() );

        one = new IntWritable(promo);
        context.write(word,one );}

}



