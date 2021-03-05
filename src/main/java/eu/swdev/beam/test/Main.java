package eu.swdev.beam.test;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class Main {

    public static Instant START = Instant.ofEpochMilli(150_000_000_000l);

    public static Logger DATA_LOGGER = LoggerFactory.getLogger("Data");
    public static Logger WATERMARK_LOGGER = LoggerFactory.getLogger("Watermark");

    public static class Data {

        private static Coder<Long> LONG_CODER = VarLongCoder.of();
        private static Coder<Instant> INSTANT_CODER = InstantCoder.of();

        public static Coder<Data> CODER = new AtomicCoder<Data>() {
            @Override
            public void encode(Data value, OutputStream outStream) throws CoderException, IOException {
                LONG_CODER.encode(value.value, outStream);
                INSTANT_CODER.encode(value.timestamp, outStream);
            }

            @Override
            public Data decode(InputStream inStream) throws CoderException, IOException {
                return new Data(
                        LONG_CODER.decode(inStream),
                        INSTANT_CODER.decode(inStream)
                );
            }
        };

        public final long value;
        public final Instant timestamp;

        public Data(long value, Instant timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }
    }

    public enum Source {

        READ_FROM {
            @Override
            PTransform<PBegin, PCollection<Data>> source(Options opts) {
                return Read.from(new StreamSource(opts));
            }
        },

        TEST_STREAM {
            @Override
            PTransform<PBegin, PCollection<Data>> source(Options opts) {
                return createTestStreamSource(opts);
            }
        }

        ;

        abstract PTransform<PBegin, PCollection<Data>> source(Options opts);
    }

    public enum DataSet {

        D1 {
            @Override
            Stream<Data> source(long offset, long limit) {
                return LongStream
                        .range(offset, limit)
                        .mapToObj(l -> new Data(l, START.plus(Duration.standardSeconds(l))));
            }
        },

        D2 {
            @Override
            Stream<Data> source(long offset, long limit) {
                return LongStream
                        .range(offset, limit)
                        .mapToObj(l -> {
                            // 3 & 8 are shifted to the next window
                            // 4 & 9 are late
                            long off = l % 5 == 3 ? 3 : 0;
                            return new Data(l, START.plus(Duration.standardSeconds(l + off)));
                        });
            }
        },

        ;

        abstract Stream<Data> source(long offset, long limit);
    }

    public interface Options extends PipelineOptions {

        @Default.Enum("TEST_STREAM")
        Source getSource();

        void setSource(Source v);

        @Default.Enum("D2")
        DataSet getDataSet();

        void setDataSet(DataSet v);

        @Default.Long(10)
        long getLimit();

        void setLimit(long v);

    }

    public static void main(String[] args) {

        Options opts = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        System.out.println("Source: " + opts.getSource() + "; DataSet: " + opts.getDataSet());

        PTransform<PBegin, PCollection<Data>> source = opts.getSource().source(opts);

        Pipeline p = Pipeline.create(opts);

        PCollection<?> coll = p.apply(source)
                .apply(
                        Window.<Data>into(FixedWindows.of(Duration.standardSeconds(5)))
                                .triggering(AfterWatermark.pastEndOfWindow())
                                .withAllowedLateness(Duration.ZERO)
                                .discardingFiredPanes()
                )
                .apply(MapElements.via(dataValue))
                .apply(Combine.<Long>globally((v1, v2) -> v1 + v2).withoutDefaults())
                .apply(ParDo.of(addOutputInfo))
        ;

        PAssert.that(coll).satisfies(iter -> {
            for (Object o: iter) {
                System.out.println("res: " + o);
            }
            return null;
        });

        PipelineResult r = p.run();

        System.out.println("state: " + r.getState());

        r.metrics().allMetrics().getCounters().forEach(
                c -> System.out.println("counter: " + c)
        );

    }

    private static SimpleFunction<Data, Long> dataValue = new SimpleFunction<Data, Long>() {
        @Override
        public Long apply(Data input) {
            return input.value;
        }
    };

    private static DoFn<Long, KV<IntervalWindow, Long>> addOutputInfo = new DoFn<Long, KV<IntervalWindow, Long>>() {
        @DoFn.ProcessElement
        public void processElement(ProcessContext c, IntervalWindow w) {
            c.output(KV.of(w, c.element()));
        }
    };


    private static PTransform<PBegin, PCollection<Data>> createTestStreamSource(Options opts) {
        Stream<Data> stream = opts.getDataSet().source(0, opts.getLimit());
        Instant watermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
        TestStream.Builder<Data> streamBuilder = TestStream.create(Data.CODER);

        for (Data d : stream.collect(Collectors.toList())) {
            final Instant timestamp = d.timestamp;
            streamBuilder = streamBuilder.addElements(TimestampedValue.of(d, timestamp));
            DATA_LOGGER.debug("next element - value: " + d.value + "; timestamp: " + timestamp);
            // Advance the watermark to the max timestamp
            if (timestamp.isAfter(watermark)) {
                WATERMARK_LOGGER.debug("advance watermark - new: " + timestamp + "; old: " + watermark);
                streamBuilder = streamBuilder.advanceWatermarkTo(timestamp);
                watermark = timestamp;
            } else if (watermark != null) {
                WATERMARK_LOGGER.debug("keep watermark - watermark: " + watermark + "; data timestamp: " + timestamp);
            }
        }

        WATERMARK_LOGGER.debug("advance watermark to infinity - new: " + BoundedWindow.TIMESTAMP_MAX_VALUE);

        return streamBuilder.advanceWatermarkToInfinity();
    }

    public static class StreamSource extends UnboundedSource<Data, StreamReader.CheckpointMark> {

        private final DataSet dataSet;
        private final long limit;

        public StreamSource(Options opts) {
            this.dataSet = opts.getDataSet();
            this.limit = opts.getLimit();
        }

        @Override
        public List<? extends UnboundedSource<Data, StreamReader.CheckpointMark>> split(int desiredNumSplits, PipelineOptions options) throws Exception {
            return Collections.singletonList(this);
        }

        @Override
        public UnboundedReader<Data> createReader(PipelineOptions options, StreamReader.CheckpointMark checkpointMark) throws IOException {
            if (checkpointMark != null) {
                return new DataReader(this, dataSet.source(checkpointMark.position, limit), checkpointMark);
            } else {
                return new DataReader(this, dataSet.source(0, limit), null);
            }
        }

        @Override
        public Coder<StreamReader.CheckpointMark> getCheckpointMarkCoder() {
            return StreamReader.CheckpointMark.CODER;
        }

        @Override
        public @UnknownKeyFor @NonNull @Initialized Coder<Data> getOutputCoder() {
            return Data.CODER;
        }
    }

    public static class DataReader extends StreamReader<Data> {
        public DataReader(UnboundedSource<Data, CheckpointMark> source, Stream<Data> stream, CheckpointMark checkpointMark) {
            super(source, stream, checkpointMark);
        }

        @Override
        public boolean advance() throws IOException {
            Instant oldWatermark = watermark;
            boolean res = super.advance();
            if (res) {
                DATA_LOGGER.debug("next element - value: " + current.value + "; timestamp: " + current.timestamp);
                if (oldWatermark.isBefore(watermark)) {
                    WATERMARK_LOGGER.debug("advance watermark - new: " + watermark + "; old: " + oldWatermark);
                } else {
                    WATERMARK_LOGGER.debug("keep watermark - watermark: " + watermark + "; data timestamp: " + current.timestamp);
                }
            } else {
                WATERMARK_LOGGER.debug("advance watermark to infinity - new: " + watermark);
            }
            return res;
        }

        @Override
        protected Instant getTimestamp(Data data) {
            return data.timestamp;
        }
    }

}
