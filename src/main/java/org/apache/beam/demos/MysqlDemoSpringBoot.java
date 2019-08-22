package org.apache.beam.demos;

import org.apache.beam.demos.common.DatasouceFieldCoder;
import org.apache.beam.demos.entity.DatasourceField;
import org.apache.beam.demos.repository.DatasourceFieldRepository;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.List;

/**
 * @Description
 * @Author liuzhiqiang
 * @Date 2019/8/12 10:46
 */
//@SpringBootApplication
public class MysqlDemoSpringBoot implements CommandLineRunner {
    @Autowired
    DatasourceFieldRepository datasourceFieldRepository;

    @Override
    public void run(String... args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(options);

        pipeline.getCoderRegistry().registerCoderForClass(DatasourceField.class, new DatasouceFieldCoder());

        DatasourceField df1 = new DatasourceField();
        df1.setIsStatus("1");
        df1.setVersionId("3");
        df1.setDataAddress("1");
        df1.setFieldEnglishName("F00002");

        DatasourceField df2 = new DatasourceField();
        df2.setIsStatus("1");
        df2.setVersionId("3");
        df2.setDataAddress("3");
        df2.setFieldEnglishName("F01926");

        List<KV<String, DatasourceField>> list = Arrays.asList(
                KV.of(df1.getFieldEnglishName(), df1), KV.of(df2.getFieldEnglishName(), df2)
        );

        pipeline.apply(Create.of(list))
                .apply(MapElements.into(TypeDescriptors.strings())
                        .via((KV<String, DatasourceField> kv)->String.format("%s,%s", kv.getKey(), kv.getValue().getDataAddress())))
                .apply(TextIO.write().to("mysql-demo").withNumShards(1));
        pipeline.run().waitUntilFinish();

        System.exit(0);
    }

    public static void main(String[] args) {
        SpringApplication.run(MysqlDemoSpringBoot.class, args);
    }
}
