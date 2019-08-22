package org.apache.beam.demos;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.text.SimpleDateFormat;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.MoreObjects.firstNonNull;

public class WriteOneFilePerWindow extends PTransform<PCollection<String>, PDone> {
  private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.hourMinute();

  private String filenamePrefix;
  @Nullable
  private Integer numShards;

  public WriteOneFilePerWindow(String filenamePrefix, Integer numShards) {
    this.filenamePrefix = filenamePrefix;
    this.numShards = numShards;
  }

  @Override
  public PDone expand(PCollection<String> input) {
    ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(filenamePrefix);
    TextIO.Write write =
        TextIO.write()
            .to(new PerWindowFiles(resource))
            .withTempDirectory(resource.getCurrentDirectory())
            .withWindowedWrites();
    if (numShards != null) {
      write = write.withNumShards(numShards);
    }
    return input.apply(write);
  }

  /**
   * A {@link FileBasedSink.FilenamePolicy} produces a base file name for a write based on metadata about the data
   * being written. This always includes the shard number and the total number of shards. For
   * windowed writes, it also includes the window and pane index (a sequence number assigned to each
   * trigger firing).
   */
  public static class PerWindowFiles extends FileBasedSink.FilenamePolicy {

    private final ResourceId baseFilename;

    public PerWindowFiles(ResourceId baseFilename) {
      this.baseFilename = baseFilename;
    }

    public String filenamePrefixForWindow(IntervalWindow window) {
      SimpleDateFormat df = new SimpleDateFormat("HH：mm");
      String prefix =
          baseFilename.isDirectory() ? "" : firstNonNull(baseFilename.getFilename(), "");
      return String.format(
          "%s-%s-%s", prefix, df.format(window.start().toDate()), df.format(window.end().toDate()));
    }

    @Override
    public ResourceId windowedFilename(
        int shardNumber,
        int numShards,
        BoundedWindow window,
        PaneInfo paneInfo,
        FileBasedSink.OutputFileHints outputFileHints) {
      IntervalWindow intervalWindow = (IntervalWindow) window;
      String filename =
          String.format(
              "%s-%s-of-%s%s",
              filenamePrefixForWindow(intervalWindow),
              shardNumber,
              numShards,
              outputFileHints.getSuggestedFilenameSuffix());
      return baseFilename
          .getCurrentDirectory()
          .resolve(filename, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
    }

    @Override
    public ResourceId unwindowedFilename(
        int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
      throw new UnsupportedOperationException("Unsupported.");
    }
  }
}