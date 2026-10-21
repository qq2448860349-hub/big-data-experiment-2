package edu.course.bigdata.index;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** word TAB <document,count>;...;<total,count>. */
public final class InvertedIndex extends Configured implements Tool {
    public static class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Set<String> stopwords = new HashSet<String>();
        private final Text word = new Text();
        private final Text posting = new Text();
        private String document;

        @Override
        protected void setup(Context context) throws IOException {
            document = ((FileSplit) context.getInputSplit()).getPath().getName();
            URI[] cache = context.getCacheFiles();
            if (cache == null || cache.length != 1) {
                throw new IOException("Exactly one stopwords cache file is required.");
            }
            InputStream stream;
            if (Files.isRegularFile(Paths.get("stopwords.txt"))) {
                stream = Files.newInputStream(Paths.get("stopwords.txt"));
            } else {
                // Fallback for local mode when a cache symlink is not created.
                Path source = new Path(cache[0].toString().split("#", 2)[0]);
                stream = source.getFileSystem(context.getConfiguration()).open(source);
            }
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.replace("\uFEFF", "").trim();
                    if (!line.isEmpty() && !line.startsWith("#")) {
                        stopwords.addAll(IndexSupport.words(line));
                    }
                }
            }
            if (stopwords.isEmpty()) {
                throw new IOException("Stopwords file must not be empty.");
            }
        }

        @Override
        protected void map(LongWritable offset, Text line, Context context)
                throws IOException, InterruptedException {
            Map<String, Long> counts = new HashMap<String, Long>();
            for (String token : IndexSupport.words(line.toString())) {
                if (stopwords.contains(token)) {
                    context.getCounter("Index", "STOPWORDS_REMOVED").increment(1);
                } else {
                    counts.put(token, counts.containsKey(token) ? counts.get(token) + 1L : 1L);
                    context.getCounter("Index", "TOKENS_INDEXED").increment(1);
                }
            }
            for (Map.Entry<String, Long> entry : counts.entrySet()) {
                word.set(entry.getKey());
                posting.set(document + "\t" + entry.getValue());
                context.write(word, posting);
            }
        }
    }

    /** Sum per-document counts; reusable regardless of combiner invocation count. */
    private static Map<String, Long> collect(Iterable<Text> values) throws IOException {
        Map<String, Long> counts = new TreeMap<String, Long>();
        for (Text value : values) {
            String record = value.toString();
            int tab = record.lastIndexOf('\t');
            if (tab < 1) {
                throw new IOException("Invalid posting: " + record);
            }
            String document = record.substring(0, tab);
            long count = Long.parseLong(record.substring(tab + 1));
            long old = counts.containsKey(document) ? counts.get(document) : 0L;
            counts.put(document, Math.addExact(old, count));
        }
        return counts;
    }

    public static class CountCombiner extends Reducer<Text, Text, Text, Text> {
        private final Text posting = new Text();

        @Override
        protected void reduce(Text word, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Map.Entry<String, Long> entry : collect(values).entrySet()) {
                posting.set(entry.getKey() + "\t" + entry.getValue());
                context.write(word, posting);
            }
        }
    }

    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
        private final Text result = new Text();

        @Override
        protected void reduce(Text word, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            result.set(IndexSupport.postings(collect(values)));
            context.write(word, result);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: InvertedIndex [generic options] <input> <output> <stopwords URI>");
            return 2;
        }
        Job job = Job.getInstance(getConf(), "experiment2-inverted-index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(IndexMapper.class);
        job.setCombinerClass(CountCombiner.class);
        job.setReducerClass(IndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.addCacheFile(new URI(args[2] + "#stopwords.txt"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new InvertedIndex(), args));
    }
}
