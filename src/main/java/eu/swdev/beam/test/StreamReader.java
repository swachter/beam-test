/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2021 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2021 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package eu.swdev.beam.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class StreamReader<T> extends UnboundedSource.UnboundedReader<T> {

    private static Logger LOG = LoggerFactory.getLogger(StreamReader.class);

    private final UnboundedSource<T, CheckpointMark> source;
    private final Iterator<T> iter;
    protected Instant watermark;
    private long position;

    protected T current = null;

    public StreamReader(UnboundedSource<T, CheckpointMark> source, Stream<T> stream, CheckpointMark checkpointMark) {
        this.source = source;
        this.iter = stream.iterator();
        this.watermark = checkpointMark != null ? checkpointMark.watermark : BoundedWindow.TIMESTAMP_MIN_VALUE;
        this.position = checkpointMark != null ? checkpointMark.position : 0;
    }

    protected abstract Instant getTimestamp(T t);

    @Override
    public boolean start() throws IOException {
        return advance();
    }

    @Override
    public boolean advance() throws IOException {
        if (iter.hasNext()) {
            current = iter.next();
            position++;
            Instant timestamp = getTimestamp(current);
            if (timestamp.isAfter(watermark)) {
                watermark = timestamp;
            }
            return true;
        } else {
            current = null;
            watermark = BoundedWindow.TIMESTAMP_MAX_VALUE;
            return false;
        }
    }

    @Override
    public Instant getWatermark() {
        return watermark;
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
        return new CheckpointMark(watermark, position);
    }

    @Override
    public UnboundedSource<T, ?> getCurrentSource() {
        return source;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
        if (current == null) {
            throw new NoSuchElementException();
        }
        return current;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
        if (current == null) {
            throw new NoSuchElementException();
        }
        return getTimestamp(current);
    }

    @Override
    public void close() throws IOException {

    }

    public static class CheckpointMark implements UnboundedSource.CheckpointMark {

        private static Coder<Instant> INSTANT_CODER = InstantCoder.of();
        private static Coder<Long> LONG_CODER = VarLongCoder.of();

        public static Coder<CheckpointMark> CODER = new CustomCoder<CheckpointMark>() {

            @Override
            public void encode(CheckpointMark value, OutputStream outStream) throws CoderException, IOException {
                INSTANT_CODER.encode(value.watermark, outStream);
                LONG_CODER.encode(value.position, outStream);
            }

            @Override
            public CheckpointMark decode(InputStream inStream) throws CoderException, IOException {
                return new CheckpointMark(
                        INSTANT_CODER.decode(inStream),
                        LONG_CODER.decode(inStream)
                );
            }
        };

        public final Instant watermark;
        public final long position;

        public CheckpointMark(Instant watermark, long position) {
            this.watermark = watermark;
            this.position = position;
        }

        @Override
        public void finalizeCheckpoint() throws IOException {

        }
    }

}
