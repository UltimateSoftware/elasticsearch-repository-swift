/*
 * Copyright 2017 Wikimedia and BigData Boutique
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wikimedia.elasticsearch.swift.util.stream;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.Randomness;
import org.junit.Before;

public class SegmentedBufferTests extends LuceneTestCase {
    private SegmentedBuffer buffer;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        buffer = new SegmentedBuffer();
    }

    public void testPutWithSizeHint(){
        byte[] data = new byte[128];

        buffer = new SegmentedBuffer(data.length);
        int written = buffer.put(data);

        assertEquals(data.length, written);
    }

    public void testPutAcrossSegments(){
        byte[] data = new byte[SegmentedBuffer.SEGMENT_SIZE*2];
        Randomness.get().nextBytes(data);
        int offset = 0;

        while (offset < data.length){
            int written = buffer.put(data, offset, data.length-offset);
            offset += written;
        }

        buffer.setPos(0);
        assertEquals(data.length, buffer.available());

        byte[] compare = new byte[data.length];
        offset = 0;

        while (offset < compare.length){
            int read = buffer.get(compare, offset, compare.length-offset);
            if (read == -1){
                break;
            }
            offset += read;
        }

        assertArrayEquals(data, compare);
    }

    public void testGetWhenBufferIsEmptyReturnsEOF(){
        assertEquals(buffer.get(), -1);
    }

    public void testSetPosInTheFirstSegment(){
        byte[] data = new byte[128];
        final int newpos = 10;
        Randomness.get().nextBytes(data);

        buffer.put(data);
        buffer.setPos(newpos);

        assertEquals(data.length - newpos, buffer.available());
        byte[] compare = new byte[data.length];
        byte[] subData = new byte[data.length-newpos];
        System.arraycopy(data, newpos, subData, 0,data.length-newpos);
        int read = buffer.get(compare);
        byte[] subCompare = new byte[read];
        System.arraycopy(compare, 0, subCompare, 0, read);

        assertEquals(data.length - newpos, read);
        assertArrayEquals(subData, subCompare);
    }

    public void testSetPosInThe2ndSegment(){
        byte[] data = new byte[128];
        buffer = new SegmentedBuffer(data.length);
        final int newpos = 10;
        Randomness.get().nextBytes(data);

        buffer.put(data);
        buffer.put(data);
        buffer.setPos(data.length+newpos);

        assertEquals(data.length - newpos, buffer.available());

        byte[] compare = new byte[data.length];
        byte[] subData = new byte[data.length-newpos];
        System.arraycopy(data, newpos, subData, 0,data.length-newpos);
        int read = buffer.get(compare);
        byte[] subCompare = new byte[read];
        System.arraycopy(compare, 0, subCompare, 0, read);

        assertEquals(data.length - newpos, read);
        assertArrayEquals(subData, subCompare);
    }
}
