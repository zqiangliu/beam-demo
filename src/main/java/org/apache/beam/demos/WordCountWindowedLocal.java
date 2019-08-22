package org.apache.beam.demos;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @Description
 * @Author liuzhiqiang
 * @Date 2019/8/7 9:23
 */
public class WordCountWindowedLocal {
    /**
     * 默认时间
     */
    static class DefaultCurrentTimestamp implements DefaultValueFactory<Long>{
        @Override
        public Long create(PipelineOptions options) {
            return System.currentTimeMillis();
        }
    }

    /**
     * 默认最大时间
     */
    static class DefaultMaxTimestamp implements DefaultValueFactory<Long>{
        @Override
        public Long create(PipelineOptions options) {
            return options.as(WindowedOptions.class).getMinTimestampMillis() + Duration.standardHours(1).getMillis();
        }
    }

    /**
     * PipelineOptions
     */
    public interface WindowedOptions extends PipelineOptions{
        @Default.String("demo1.txt")
        String getInputFile();

        void setInputFile(String inputFile);

        @Default.String("demo1-output.txt")
        String getOutput();

        void setOutput(String output);

        @Default.Integer(10)
        Integer getWindowSize();

        void setWindowSize(Integer windowSize);

        @Default.InstanceFactory(DefaultMaxTimestamp.class)
        Long getMaxTimestampMillis();

        void setMaxTimestampMillis(Long timestampMillis);

        @Default.InstanceFactory(DefaultCurrentTimestamp.class)
        Long getMinTimestampMillis();

        void setMinTimestampMillis(Long timestampMillis);

        @Default.Integer(2)
        Integer getNumShards();

        void setNumShards(Integer numShards);
    }

    /**
     * 添加时间戳
     */
    static class AddTimestampFn extends DoFn<String, String>{

        private Long minTimestamp, maxTimestamp;

        AddTimestampFn(Long minTimestamp, Long maxTimestamp){
            this.minTimestamp = minTimestamp;
            this.maxTimestamp = maxTimestamp;
        }
        @ProcessElement
        public void addTimestamp(@Element String element, OutputReceiver<String> receiver){
            Instant randomTimestamp = new Instant(ThreadLocalRandom.current().nextLong(minTimestamp, maxTimestamp));
            receiver.outputWithTimestamp(element, randomTimestamp);
        }
    }

    public static void main(String[] args) {
        WindowedOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WindowedOptions.class);

        options.setRunner(DirectRunner.class);

        System.out.printf("minTimestamp=%d,maxTimestamp=%d,numShards=%d", options.getMinTimestampMillis(), options.getMaxTimestampMillis(), options.getNumShards());

        Pipeline pipeline = Pipeline.create(options);

        //添加时间戳
        PCollection<String> inputs = pipeline.apply(TextIO.read().from(options.getInputFile()))
                .apply(ParDo.of(new AddTimestampFn(options.getMinTimestampMillis(), options.getMaxTimestampMillis())));

        //分割成窗口
        PCollection<String> windowedResults = inputs.apply(Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))));

        //计算单词数量
        PCollection<KV<String, Long>> wordCounts = windowedResults.apply(new WordCountLocal.CountWords());

        //分窗口输出
        wordCounts.apply(MapElements.via(new WordCountLocal.FormatAsTextFn()))
                .apply(new WriteOneFilePerWindow(options.getOutput(), options.getNumShards()));

        pipeline.run().waitUntilFinish();
    }
}
