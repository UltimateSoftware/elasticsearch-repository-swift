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

import java.util.LinkedList;

// class meant to be package private.
// This class is not re-entrant
class SegmentedBuffer {
    public static final int SEGMENT_SIZE = 1024*1024;

    private final LinkedList<byte[]> segments = new LinkedList<>();
    private long pos = 0;
    private long stored = 0;

    SegmentedBuffer() {
    }

    SegmentedBuffer(long sizeHint) {
        // handle small-ish buffers. Other hint values else will use default segments
        if (sizeHint > 0 && sizeHint < SEGMENT_SIZE){
            addSegment(sizeHint);
        }
    }

    private void addSegment(long size){
        segments.add(new byte[(int) size]);
    }

    private static class SegmentPosInfo {
        byte[] segment;
        long pos;
        long availableToPut;
        long availableToGet;
    }

    private SegmentPosInfo getCurrentSegmentInfo(){
        SegmentPosInfo result = new SegmentPosInfo();

        if (segments.isEmpty()){
            addSegment(SEGMENT_SIZE);
        }

        byte[] firstSegment = segments.get(0);
        if (pos < firstSegment.length) {
            result.segment = firstSegment;
            result.pos = pos;
            result.availableToPut = firstSegment.length - pos;
            result.availableToGet = Math.min(stored - pos, firstSegment.length - pos);
            return result;
        }

        long segIndex = (pos - firstSegment.length)/SEGMENT_SIZE + 1;
        if (segIndex < segments.size()) {
            result.segment = segments.get((int) segIndex);
            result.availableToPut = SEGMENT_SIZE - result.pos;
            result.availableToGet = Math.min(stored - pos, SEGMENT_SIZE - result.pos);
        }
        result.pos = (pos - firstSegment.length)%SEGMENT_SIZE;

        return result;
    }

    public long available() {
        return stored - pos;
    }

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        if (pos < 0 || pos > stored){
            throw new IllegalArgumentException("pos cannot exceed stored size or be negative");
        }
        this.pos = pos;
    }

    public int put(final byte[] b, final int off, final int len){
        final SegmentPosInfo currentSegment = getCurrentSegmentInfo();
        if (currentSegment.availableToPut == 0){
            addSegment(SEGMENT_SIZE);
            currentSegment.segment = segments.getLast();
            currentSegment.pos = 0;
            currentSegment.availableToPut = SEGMENT_SIZE;
        }

        int written = Math.min((int)currentSegment.availableToPut, len);
        System.arraycopy(b, off, currentSegment.segment, (int)currentSegment.pos, written);
        pos += written;
        stored += written;
        return written;
    }

    public void put(byte b){
        final SegmentPosInfo currentSegment = getCurrentSegmentInfo();
        if (currentSegment.availableToPut == 0){
            addSegment(SEGMENT_SIZE);
            currentSegment.segment = segments.getLast();
            currentSegment.pos = 0;
            currentSegment.availableToPut = SEGMENT_SIZE;
        }

        currentSegment.segment[(int) currentSegment.pos] = b;
        pos++;
        stored++;
    }

    public int put(final byte[] b) {
        return put(b, 0, b.length);
    }

    public int get(final byte[] b, final int off, final int len){
        if (available() == 0){
            return -1;
        }

        final SegmentPosInfo currentSegment = getCurrentSegmentInfo();
        int read = (int) Math.min(currentSegment.availableToGet, len);
        System.arraycopy(currentSegment.segment, (int) currentSegment.pos, b, off, read);
        pos += read;
        return read;
    }

    public int get(final byte[] b){
        return get(b, 0, b.length);
    }

    public int get(){
        if (available() == 0){
            return -1;
        }

        final SegmentPosInfo currentSegment = getCurrentSegmentInfo();
        int result = currentSegment.segment[(int) currentSegment.pos];
        pos++;
        return result;
    }
}
