package org.apache.beam.demos;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;

/**
 * @Description WordCount example using DoFn
 * @Author liuzhiqiang
 * @Date 2019/8/6 16:13
 */
public class WordCountLocal {
    /**
     * This DoFn tokenize lines of text into individual words
     */
    static class ExtractWordsFn extends DoFn<String, String>{
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<String> receiver){
            String[] words = line.split("\\s+");
            for(String word : words){
                if(!StringUtils.isEmpty(word)){
                    receiver.output(word);
                }
            }
        }
    }

    /**
     * format kv(word-count) to text word:count
     */
    static class FormatAsTextFn extends SimpleFunction<KV<String,Long>, String>{
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ":" + input.getValue();
        }
    }

    /**
     * transform words to word count
     */
    static class CountWords extends PTransform<PCollection<String>, PCollection<KV<String,Long>>>{
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> input) {
            return input.apply(ParDo.of(new ExtractWordsFn())).apply(Count.perElement());
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(options);

//        pipeline.apply(TextIO.read().from("demo1.txt"))
//                .apply(new CountWords())
//                .apply(MapElements.via(new FormatAsTextFn()))
//                .apply(TextIO.write().to("demo1-output"));

        //1. read
        pipeline.apply(TextIO.read().from("demo1.txt"))
                //2. split lines into words
                .apply(ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void doSplit(@Element String element, OutputReceiver<String> receiver){
                        String[] words = element.split("\\s+");
                        for(String word : words){
                            receiver.output(word);
                        }
                    }
                }))
                //3. count word
                .apply(Count.perElement())
                //4. transform word count kv object to string
                .apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                    @Override
                    public String apply(KV<String, Long> input) {
                        return input.getKey() + ":" + input.getValue();
                    }
                }))
                //5. output to file
                .apply(TextIO.write().to("demo1-output"));

        pipeline.run().waitUntilFinish();

        System.out.println("job complete!");
    }
}
