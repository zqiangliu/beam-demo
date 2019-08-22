package org.apache.beam.demos;

import org.apache.beam.demos.common.DateFormat;
import org.apache.beam.demos.common.DateUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.Date;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * @Description 使用window和combine统计每小时的数据 window combine
 * @Author liuzhiqiang
 * @Date 2019/8/8 16:33
 */
public class WindowTriggerDemo {

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(TextIO.read().from("0157_20190722000000_20190722235959.csv"))
                .apply(ParDo.of(new DoFn<String, KV<String, String>>() {
                    @ProcessElement
                    public void processElement(@Element String line, OutputReceiver<KV<String, String>> receiver){
                        String[] cells = line.split(",");
                        String time = cells[0].replaceAll("\"", "");
                        String urgency_brake = cells[7].replaceAll("\"", "");
                        if(!Pattern.matches("^\\d+.+", time)){
                            return;
                        }
                        Date datetime = DateUtil.parseDate(time, DateFormat.YYYYMMDDHHMMSS);
                        String hour = datetime.getHours() + "时";
                        receiver.outputWithTimestamp(KV.of(hour, 1 + "," + urgency_brake), new Instant(datetime.getTime()));
                    }
                }))
                .apply(
                        Window.<KV<String, String>>into(FixedWindows.of(Duration.standardHours(1)))
                                .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                                .withAllowedLateness(Duration.ZERO)
                                .discardingFiredPanes()
                )
                .apply(Combine.perKey(new CountHour()))
                //.apply(GroupByKey.create())
                .apply(MapElements.via(new FormatFn()))
                //.apply(TextIO.write().to("0157-output").withNumShards(1));
                .apply(new WriteOneFilePerWindow("0157-output", 1));
        pipeline.run().waitUntilFinish();
    }

    static class FormatFn extends SimpleFunction<KV<String, String>, String>{
        @Override
        public String apply(KV<String, String> input) {
            String[] arr = input.getValue().split(",");
            return input.getKey() + ":total=" + arr[0] + ",urgency_brake=" + arr[1];
        }
    }

    static class CountHour implements SerializableFunction<Iterable<String>, String>{
        @Override
        public String apply(Iterable<String> input) {
            Iterator<String> it = input.iterator();
            Integer total = 0;
            Integer urgency_brake_sum = 0;
            while(it.hasNext()){
                String[] arr = it.next().split(",");
                total += Integer.parseInt(arr[0]);
                urgency_brake_sum += Integer.parseInt(arr[1]);
            }
            return total + "," + urgency_brake_sum;
        }
    }
}
