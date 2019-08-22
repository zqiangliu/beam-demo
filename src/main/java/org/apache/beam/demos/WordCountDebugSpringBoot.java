package org.apache.beam.demos;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.List;

/**
 * @Description
 * @Author liuzhiqiang
 * @Date 2019/8/6 17:49
 */
//@SpringBootApplication
public class WordCountDebugSpringBoot implements CommandLineRunner {
    @Override
    public void run(String... args) {
        WordCountDebugLocal.WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(WordCountDebugLocal.WordCountOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        //calculate word count, and filter some specified keys
        PCollection<KV<String, Long>> filteredWords = pipeline.apply(TextIO.read().from(options.getInputFile()))
                .apply(new WordCountLocal.CountWords())
                .apply(ParDo.of(new WordCountDebugLocal.FilterFn(options.getFilterPattern())));

        //expected results
        List<KV<String, Long>> expectedResults = Arrays.asList(KV.of("apache", 1L), KV.of("beam", 3L));

        PAssert.that(filteredWords).containsInAnyOrder(expectedResults);

        pipeline.run().waitUntilFinish();

        System.out.println("job complete!");

        System.exit(0);
    }

    public static void main(String[] args) {
        SpringApplication.run(WordCountDebugSpringBoot.class, args);
    }
}
