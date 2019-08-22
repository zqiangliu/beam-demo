package org.apache.beam.demos;

import org.apache.beam.demos.common.MyCoder;
import org.apache.beam.demos.entity.DatasourceField;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.sql.ResultSet;

/**
 * @Description
 * @Author liuzhiqiang
 * @Date 2019/8/22 16:54
 */
public class JdbcDemo {
    public static void main(String[] args) throws IOException{
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        Pipeline pipeline = Pipeline.create(options);

        //query from mysql, and output a csv file with fields specified
        pipeline.apply(JdbcIO.<DatasourceField>read()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                        "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/metro")
                .withUsername("root")
                .withPassword("root"))
                .withQuery("select field_id, field_english_name, version_id, data_address, is_status from dim_datasource_field_define where is_status='1' and version_id='3' and is_delete='0'")
                .withCoder(new MyCoder<>())
                .withRowMapper((ResultSet rs) -> {
                    DatasourceField df = new DatasourceField();
                    df.setFieldId(rs.getInt(1));
                    df.setFieldEnglishName(rs.getString(2));
                    df.setVersionId(rs.getString(3));
                    df.setDataAddress(rs.getString(4));
                    df.setIsStatus(rs.getString(5));
                    return df;
                }))
                .apply(FileIO.<DatasourceField>write()
                        .via(new CsvFileSink("字段ID,字段名,位置,版本,状态"))
                        .to("jdbc-demo")
                        .withPrefix("jdbc-demo")
                        .withSuffix(".csv")
                        .withNumShards(1));

        pipeline.run().waitUntilFinish();

    }

    //csv file formatter
    static class CsvFileSink implements FileIO.Sink<DatasourceField>{
        private String headers;
        private PrintWriter writer;

        public CsvFileSink(String headers){
            this.headers = headers;
        }
        @Override
        public void open(WritableByteChannel channel) throws IOException {
            writer = new PrintWriter(Channels.newOutputStream(channel));
            writer.println(headers);
        }

        @Override
        public void write(DatasourceField element) throws IOException {
            writer.println(String.format("%d,%s,%s,%s,%s", element.getFieldId(), element.getFieldEnglishName(),
                    element.getDataAddress(), element.getVersionId(), element.getIsStatus()));
        }

        @Override
        public void flush() throws IOException {
            writer.flush();
        }
    }
}
