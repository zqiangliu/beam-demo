package org.apache.beam.demos;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import javax.annotation.Nullable;

/**
 * @Description
 * @Author liuzhiqiang
 * @Date 2019/8/9 15:43
 */
public class MultiOutputDemo {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();

        Pipeline pipeline = Pipeline.create(options);

        //定义元素标签
        TupleTag<String> numSmallTag = new TupleTag<>();
        TupleTag<String> numMediumTag = new TupleTag<>();
        TupleTag<String> numLargeTag = new TupleTag<>();

        //创建输入数据
        PCollection<Integer> nums = pipeline.apply(Create.of(1, 4, 5, 2, 11, 9, 21, 101, 31, 99, 41, 3, 12, 22, 189, 6, 45));
        //多路输出
        PCollectionTuple collectionTuple = nums.apply(ParDo.of(new DoFn<Integer, String>() {

                @ProcessElement
                public void processElement(@Element Integer element, MultiOutputReceiver receiver){
                    if(element < 10){
                        receiver.get(numSmallTag).output(element.toString());
                    }else if(element >= 10 && element < 50){
                        receiver.get(numMediumTag).output(element.toString());
                    }else{
                        receiver.get(numLargeTag).output(element.toString());
                    }
                }
            }).withOutputTags(numSmallTag, TupleTagList.of(numMediumTag).and(numLargeTag)));

        //将输出写入到文件
        ResourceId tempDirectory = FileBasedSink.convertToFileResourceIfPossible("temp");

        collectionTuple.get(numSmallTag)
                .setCoder(StringUtf8Coder.of())
                .apply(TextIO.write().to(new OutputFilenamePolicy("small-num")).withTempDirectory(tempDirectory).withNumShards(1));
        collectionTuple
                .get(numMediumTag)
                .setCoder(StringUtf8Coder.of())
                .apply(TextIO.write().to(new OutputFilenamePolicy("medium-num")).withTempDirectory(tempDirectory).withNumShards(1));
        collectionTuple
                .get(numLargeTag)
                .setCoder(StringUtf8Coder.of())
                .apply(TextIO.write().to(new OutputFilenamePolicy("large-num")).withTempDirectory(tempDirectory).withNumShards(1));

        pipeline.run().waitUntilFinish();
    }

    static class OutputFilenamePolicy extends FileBasedSink.FilenamePolicy{

        private ResourceId outputResourceId;

        public OutputFilenamePolicy(String filePrefix){
            this.outputResourceId = FileBasedSink.convertToFileResourceIfPossible(filePrefix);
        }

        @Override
        public ResourceId windowedFilename(int shardNumber, int numShards, BoundedWindow window, PaneInfo paneInfo, FileBasedSink.OutputFileHints outputFileHints) {
            return this.outputResourceId;
        }

        @Nullable
        @Override
        public ResourceId unwindowedFilename(int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
            return this.outputResourceId;
        }
    }
}
