package org.apache.beam.demos;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @Description
 * @Author liuzhiqiang
 * @Date 2019/8/6 17:11
 */
public class WordCountDebugLocal {
    public interface WordCountOptions extends PipelineOptions{
        @Default.String("demo1.txt")
        String getInputFile();

        void setInputFile(String inputFile);

        //required表示output参数为必选，即使没有输出
        @Validation.Required
        String getOutput();

        void setOutput(String output);

        @Default.String("apache|beam")
        String getFilterPattern();

        void setFilterPattern(String pattern);
    }

    static class FilterFn extends DoFn<KV<String, Long>, KV<String, Long>>{
        static Logger logger = LoggerFactory.getLogger(FilterFn.class);

        private String pattern;
        public FilterFn(String pattern){
            this.pattern = pattern;
        }
        @ProcessElement
        public void doFilter(ProcessContext context){
            String key = context.element().getKey();
            if(Pattern.matches(pattern, key)){
                logger.info("matched:" + key);
                context.output(context.element());
            }else{
                logger.info("unmatched:" + key);
            }
        }
    }

    public static void main(String[] args) {
        //fromArgs表示可以从运行参数获取options设置
        WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        //calculate word count, and filter some specified keys
        PCollection<KV<String, Long>> filteredWords = pipeline.apply(TextIO.read().from(options.getInputFile()))
                .apply(new WordCountLocal.CountWords())
                .apply(ParDo.of(new FilterFn(options.getFilterPattern())));

        //expected results
        List<KV<String, Long>> expectedResults = Arrays.asList(KV.of("apache", 1L), KV.of("beam", 3L));

        PAssert.that(filteredWords).containsInAnyOrder(expectedResults);

        pipeline.run().waitUntilFinish();

    }
}
