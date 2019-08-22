package org.apache.beam.demos;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;

/**
 * @Description
 * @Author liuzhiqiang
 * @Date 2019/8/9 17:30
 */
public class AggregationDemo {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();

        Pipeline pipeline = Pipeline.create(options);

        //全局平均值
        pipeline.apply(Create.of(1, 2, 3, 5, 7, 11, 4, 6, 21))
                .apply(Mean.globally())
                .apply(ParDo.of(new DoFn<Double, String>() {
                    @ProcessElement
                    public void processElement(@Element Double element, OutputReceiver<String> receiver){
                        receiver.output(DecimalFormat.getInstance().format(element));
                    }
                }))
                .apply(TextIO.write().to("mean-output1").withNumShards(1));


        //分组平均值
        List<KV<String, Integer>> groupNums = Arrays.asList(
                KV.of("group1", 1), KV.of("group1", 2),
                KV.of("group1", 3), KV.of("group1", 4),
                KV.of("group2", 5), KV.of("group2", 6),
                KV.of("group2", 7), KV.of("group2", 8)
        );

        pipeline
                .apply(Create.of(groupNums))
                .apply(Mean.perKey())
                .apply(MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Double> kv)->kv.getKey()+":"+kv.getValue()))
                .apply(TextIO.write().to("mean-output2").withNumShards(1));

        pipeline.run().waitUntilFinish();
    }

    static class KVFormatFn extends SimpleFunction<KV<String, Double>, String>{
        @Override
        public String apply(KV<String, Double> input) {
            return input.getKey() + ":" + input.getValue();
        }
    }
}
