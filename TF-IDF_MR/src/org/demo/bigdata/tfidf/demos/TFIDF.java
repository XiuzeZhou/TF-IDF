package org.demo.bigdata.tfidf.demos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TFIDF {
    // part1------------------------------------------------------------------------
    public static class Mapper_Part1 extends Mapper<LongWritable, Text, Text, Text> {
        String File_name = ""; // 淇濆瓨鏂囦欢鍚嶏紝鏍规嵁鏂囦欢鍚嶅尯鍒嗘墍灞炴枃浠�
        int all = 0; // 鍗曡瘝鎬绘暟缁熻
        static Text one = new Text("1");
        String word;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            String str = ((FileSplit) inputSplit).getPath().toString();
            File_name = str.substring(str.lastIndexOf("/") + 1); // 鑾峰彇鏂囦欢鍚�
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word = File_name;
                word += " ";
                word += itr.nextToken(); // 灏嗘枃浠跺悕鍔犲崟璇嶄綔涓簁ey es: test1 hello 1
                all++;
                context.write(new Text(word), one);
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            // Map鐨勬渶鍚庯紝鎴戜滑灏嗗崟璇嶇殑鎬绘暟鍐欏叆銆備笅闈㈤渶瑕佺敤鎬诲崟璇嶆暟鏉ヨ绠椼��
            String str = "";
            str += all;
            context.write(new Text(File_name + " " + "!"), new Text(str));
            // 涓昏杩欓噷鍊间娇鐢ㄧ殑 "!"鏄壒鍒瀯閫犵殑銆� 鍥犱负!鐨刟scii姣旀墍鏈夌殑瀛楁瘝閮藉皬銆�
        }
    }

    public static class Combiner_Part1 extends Reducer<Text, Text, Text, Text> {
        float all = 0;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int index = key.toString().indexOf(" ");
            // 鍥犱负!鐨刟scii鏈�灏忥紝鎵�浠ュ湪map闃舵鐨勬帓搴忓悗锛�!浼氬嚭鐜板湪绗竴涓�
            if (key.toString().substring(index + 1, index + 2).equals("!")) {
                for (Text val : values) {
                    // 鑾峰彇鎬荤殑鍗曡瘝鏁般��
                    all = Integer.parseInt(val.toString());
                }
                // 杩欎釜key-value琚姏寮�
                return;
            }
            float sum = 0; // 缁熻鏌愪釜鍗曡瘝鍑虹幇鐨勬鏁�
            for (Text val : values) {
                sum += Integer.parseInt(val.toString());
            }
            // 璺冲嚭寰幆鍚庯紝鏌愪釜鍗曡瘝鏁板嚭鐜扮殑娆℃暟灏辩粺璁″畬浜嗭紝鎵�鏈� TF(璇嶉) = sum / all
            float tmp = sum / all;
            String value = "";
            value += tmp; // 璁板綍璇嶉
            // 灏唊ey涓崟璇嶅拰鏂囦欢鍚嶈繘琛屼簰鎹€�俥s: test1 hello -> hello test1
            String p[] = key.toString().split(" ");
            String key_to = "";
            key_to += p[1];
            key_to += " ";
            key_to += p[0];
            context.write(new Text(key_to), new Text(value));
        }
    }

    public static class Reduce_Part1 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    public static class MyPartitoner extends Partitioner<Text, Text> {
        // 瀹炵幇鑷畾涔夌殑Partitioner
        public int getPartition(Text key, Text value, int numPartitions) {
            // 鎴戜滑灏嗕竴涓枃浠朵腑璁＄畻鐨勭粨鏋滀綔涓轰竴涓枃浠朵繚瀛�
            // es锛� test1 test2
            String ip1 = key.toString();
            ip1 = ip1.substring(0, ip1.indexOf(" "));
            Text p1 = new Text(ip1);
            return Math.abs((p1.hashCode() * 127) % numPartitions);
        }
    }

    // part2-----------------------------------------------------
    public static class Mapper_Part2 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString().replaceAll("    ", " "); // 灏唙laue涓殑TAB鍒嗗壊绗︽崲鎴愮┖鏍�
            // es: Bank
            // test1
            // 0.11764706 ->
            // Bank test1
            // 0.11764706
            int index = val.indexOf(" ");
            String s1 = val.substring(0, index); // 鑾峰彇鍗曡瘝 浣滀负key es: hello
            String s2 = val.substring(index + 1); // 鍏朵綑閮ㄥ垎 浣滀负value es: test1
            // 0.11764706
            s2 += " ";
            s2 += "1"; // 缁熻鍗曡瘝鍦ㄦ墍鏈夋枃绔犱腑鍑虹幇鐨勬鏁�, 鈥�1鈥� 琛ㄧず鍑虹幇涓�娆°�� es: test1 0.11764706 1
            context.write(new Text(s1), new Text(s2));
        }
    }

    public static class Reduce_Part2 extends Reducer<Text, Text, Text, Text> {
        int file_count;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 鍚屼竴涓崟璇嶄細琚垎鎴愬悓涓�涓猤roup
            file_count = context.getNumReduceTasks(); // 鑾峰彇鎬绘枃浠舵暟
            float sum = 0;
            List<String> vals = new ArrayList<String>();
            for (Text str : values) {
                int index = str.toString().lastIndexOf(" ");
                sum += Integer.parseInt(str.toString().substring(index + 1)); // 缁熻姝ゅ崟璇嶅湪鎵�鏈夋枃浠朵腑鍑虹幇鐨勬鏁�
                vals.add(str.toString().substring(0, index)); // 淇濆瓨
            }
            double tmp = Math.log10(file_count * 1.0 / (sum * 1.0)); // 鍗曡瘝鍦ㄦ墍鏈夋枃浠朵腑鍑虹幇鐨勬鏁伴櫎浠ユ�绘枃浠舵暟
                                                                     // = IDF
            for (int j = 0; j < vals.size(); j++) {
                String val = vals.get(j);
                String end = val.substring(val.lastIndexOf(" "));
                float f_end = Float.parseFloat(end); // 璇诲彇TF
                val += " ";
                val += f_end * tmp; // tf-idf鍊�
                context.write(key, new Text(val));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // part1----------------------------------------------------
        Configuration conf1 = new Configuration();
        // 璁剧疆鏂囦欢涓暟锛屽湪璁＄畻DF(鏂囦欢棰戠巼)鏃朵細浣跨敤
        FileSystem hdfs = FileSystem.get(conf1);
        FileStatus p[] = hdfs.listStatus(new Path(args[0]));
        // 鑾峰彇杈撳叆鏂囦欢澶瑰唴鏂囦欢鐨勪釜鏁帮紝鐒跺悗鏉ヨ缃甆umReduceTasks
        Job job1 = Job.getInstance(conf1, "My_tdif_part1");
        job1.setJarByClass(TFIDF.class);
        job1.setMapperClass(Mapper_Part1.class);
        job1.setCombinerClass(Combiner_Part1.class); // combiner鍦ㄦ湰鍦版墽琛岋紝鏁堢巼瑕侀珮鐐广��
        job1.setReducerClass(Reduce_Part1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(p.length);
        job1.setPartitionerClass(MyPartitoner.class); // 浣跨敤鑷畾涔塎yPartitoner

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);
        // part2----------------------------------------
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "My_tdif_part2");
        job2.setJarByClass(TFIDF.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapperClass(Mapper_Part2.class);
        job2.setReducerClass(Reduce_Part2.class);
        job2.setNumReduceTasks(p.length);
        FileInputFormat.setInputPaths(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);
        // hdfs.delete(new Path(args[1]), true);
    }
}