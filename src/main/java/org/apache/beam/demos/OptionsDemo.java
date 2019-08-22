package org.apache.beam.demos;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * @Description Options Demo, set default options value , change program arguments(ex. --name=apache-beam) and see output change
 * @Author liuzhiqiang
 * @Date 2019/8/12 9:22
 */
public class OptionsDemo {
    public interface DemoOptions extends PipelineOptions{
        @Description("demo options name")
        @Default.String("hello apache beam")
        ValueProvider<String> getName();

        void setName(ValueProvider<String> name);

    }
    public static void main(String[] args) {
        DemoOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DemoOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(Create.of(1, 2, 3)).apply(ParDo.of(new DoFn<Integer, String>() {
            @ProcessElement
            public void processElement(ProcessContext context){
                DemoOptions demoOptions = context.getPipelineOptions().as(DemoOptions.class);
                context.output(demoOptions.getName() + "-" + context.element());
            }
        })).apply(TextIO.write().to("options-demo").withNumShards(1));

        pipeline.run().waitUntilFinish();
    }
}
