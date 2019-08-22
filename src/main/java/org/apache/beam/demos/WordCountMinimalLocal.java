package org.apache.beam.demos;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang.StringUtils;
import java.util.Arrays;

/**
 * WordCount example with local file based input and output by direct runner
 * @Description
 * @Author liuzhiqiang
 * @Date 2019/8/6 15:28
 */
public class WordCountMinimalLocal {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        //options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(TextIO.read().from("demo1.txt"))
                .apply(FlatMapElements.into(TypeDescriptors.strings()).via((String line)-> Arrays.asList(line.split("\\s+"))))
                .apply(Filter.by((String word)->!StringUtils.isEmpty(word)))
                .apply(Count.perElement())
                .apply(MapElements.into(TypeDescriptors.strings()).via((KV<String, Long> wordCount)->wordCount.getKey() + ":" + wordCount.getValue()))
                .apply(TextIO.write().to("demo1-output"));
        //write().to("/filepath") //current root directory
        //write().to("filepath") //working director
        pipeline.run().waitUntilFinish();
        System.out.println(System.getProperty("user.dir"));
        System.out.println(System.getProperty("user.home"));
    }
}
