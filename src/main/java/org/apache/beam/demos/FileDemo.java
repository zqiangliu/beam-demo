package org.apache.beam.demos;

import com.alibaba.fastjson.JSON;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

import java.io.IOException;

/**
 * @Description
 * @Author liuzhiqiang
 * @Date 2019/8/22 18:09
 */
public class FileDemo {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline pipeline = Pipeline.create(options);
        //read files match the pattern
        pipeline.apply(FileIO.match().filepattern("demo*.txt"))
                .apply(FileIO.readMatches())
                .apply(MapElements.via(new SimpleFunction<FileIO.ReadableFile, KV<String, String>>() {
                    @Override
                    public KV apply(FileIO.ReadableFile f) {
                        try {
                            //kv of filename, file content
                            return KV.of(f.getMetadata().resourceId().toString(), f.readFullyAsUTF8String());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return KV.of("", "");
                    }
                }))
                .apply(ParDo.of(new DoFn<KV<String, String>, String>() {
                    @ProcessElement
                    public void process(@Element KV<String, String> input, OutputReceiver<String> receiver){
                        receiver.output(JSON.toJSONString(input));
                    }
                }))
                .apply(TextIO.write()
                        .to("readfile")
                        .withNumShards(1));

        pipeline.run().waitUntilFinish();
    }
}
