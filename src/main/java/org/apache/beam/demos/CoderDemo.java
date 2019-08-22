package org.apache.beam.demos;

import org.apache.beam.demos.common.ScoreCoder;
import org.apache.beam.demos.entity.Score;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

import java.text.DecimalFormat;
import java.util.*;

/**
 * @Description
 * @Author liuzhiqiang
 * @Date 2019/8/19 9:16
 */
public class CoderDemo {
    static class KeyFn implements SerializableFunction<Score, Long>{
        @Override
        public Long apply(Score input) {
            return input.getStudentId();
        }
    }
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();

        Pipeline pipeline = Pipeline.create(options);

        //pipeline.getCoderRegistry().registerCoderForClass(Iterable.class, IterableCoder.of(StringUtf8Coder.of()));
        //pipeline.getCoderRegistry().registerCoderForClass(Score.class, new ScoreCoder());


        List<Score> list = Arrays.asList(
                new Score(1L, 1L, 60F),
                new Score(1L, 2L, 85.5F),
                new Score(1L, 3L, 98F),
                new Score(2L, 1L, 93F),
                new Score(2L, 2L, 78F),
                new Score(2L, 3L, 87F),
                new Score(3L, 1L, 91F),
                new Score(3L, 2L, 92F),
                new Score(3L, 3L, 88F),
                new Score(4L, 1L, 97F),
                new Score(4L, 2L, 95F),
                new Score(4L, 3L, 100F)
        );

        pipeline.apply(Create.of(list))
                .apply(ParDo.of(new DoFn<Score, KV<Long, Score>>() {
                    @ProcessElement
                    public void processElement(@Element Score score, OutputReceiver<KV<Long, Score>> receiver){
                        receiver.output(KV.of(score.getStudentId(), score));
                    }
                }))
                .apply(GroupByKey.create())
                .apply(ParDo.of(new DoFn<KV<Long, Iterable<Score>>, String>() {
                    @ProcessElement
                    public void processElement(@Element KV<Long, Iterable<Score>> input, OutputReceiver<String> receiver){
                        StringBuffer sb = new StringBuffer();
                        sb.append("学生").append(input.getKey()).append(",");
                        double total = 0;
                        for(Score s : input.getValue()){
                            total += s.getScore();
                            switch (s.getSubjectId().toString()){
                                case "1":
                                    sb.append("语文:").append(s.getScore());
                                    break;
                                case "2":
                                    sb.append("数学:").append(s.getScore());
                                    break;
                                case "3":
                                    sb.append("英语:").append(s.getScore());
                                    break;
                                default:
                                    break;
                            }
                        }
                        sb.append("平均分:").append(new DecimalFormat().format(total / 3));
                        receiver.output(sb.toString());
                    }
                }))
                .apply(TextIO.write().to("coder-demo").withNumShards(1));

        pipeline.run().waitUntilFinish();
    }


}
