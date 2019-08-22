package org.apache.beam.demos;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;

import java.text.DecimalFormat;
import java.util.HashMap;

/**
 * @Description
 * @Author liuzhiqiang
 * @Date 2019/8/9 14:40
 */
public class CombineDemo {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(Create.of(1, 4, 5, 2, 11, 9, 21, 101, 31, 118, 26, 999))
//                .apply(Sum.integersGlobally())
                .apply(Combine.globally(new Combine.CombineFn<Integer, HashMap<String, Integer>, String>() {
                    @Override
                    public HashMap<String, Integer> createAccumulator() {
                        HashMap<String, Integer> accum = new HashMap<>();
                        accum.put("sum", 0);
                        accum.put("count", 0);
                        accum.put("gt100", 0);
                        return accum;
                    }

                    @Override
                    public HashMap<String, Integer> addInput(HashMap<String, Integer> mutableAccumulator, Integer input) {
                        mutableAccumulator.put("sum", mutableAccumulator.get("sum") + input);
                        mutableAccumulator.put("count", mutableAccumulator.get("count") + 1);
                        if(input > 100){
                            mutableAccumulator.put("gt100", mutableAccumulator.get("gt100") + 1);
                        }
                        return mutableAccumulator;
                    }

                    @Override
                    public HashMap<String, Integer> mergeAccumulators(Iterable<HashMap<String, Integer>> accumulators) {
                        HashMap<String, Integer> merged = createAccumulator();
                        for(HashMap<String, Integer> acc : accumulators){
                            merged.put("sum", merged.get("sum") + acc.get("sum"));
                            merged.put("count", merged.get("count") + acc.get("count"));
                            merged.put("gt100", merged.get("gt100") + acc.get("gt100"));
                        }
                        return merged;
                    }

                    @Override
                    public String extractOutput(HashMap<String, Integer> accumulator) {
                        String mean = DecimalFormat.getInstance().format((double)accumulator.get("sum") / accumulator.get("count"));
                        return String.format("count=%d,sum=%d,mean=%s,gt100=%d", accumulator.get("count"), accumulator.get("sum"), mean, accumulator.get("gt100"));
                    }
                }))
                .apply(TextIO.write().to("combine-output"));

        pipeline.run().waitUntilFinish();
    }

}
