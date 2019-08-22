package org.apache.beam.demos;


import avro.shaded.com.google.common.collect.ImmutableList;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.hadoop.conf.Configuration;


/**
 * @Description
 * @Author liuzhiqiang
 * @Date 2019/8/5 15:24
 */
public class HdfsReadWriteDemo {
    public static void main(String[] args) {

        //
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://localhost:9000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        HadoopFileSystemOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(HadoopFileSystemOptions.class);
        options.setHdfsConfiguration(ImmutableList.of(conf));
        options.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply(TextIO.read().from("hdfs://localhost:9000/data/demos/demo1"))
                .apply(Filter.by((String input) -> input.length() > 3))
                .apply(TextIO.write().to("hdfs://localhost:9000/data/demos/demo1_output").withNumShards(1));
        pipeline.run().waitUntilFinish();

    }


}
