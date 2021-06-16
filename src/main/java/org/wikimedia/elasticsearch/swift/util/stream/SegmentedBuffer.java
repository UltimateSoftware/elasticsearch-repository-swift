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
public class SegmentedBuffer {
    public static final int SEGMENT_SIZE = 1024*1024;

    private LinkedList<byte[]> segments = new LinkedList<>();
    private byte[] currentSegment = null;
    private long pos = 0;
    private int posInSegment = -1;
    private int segmentIndex = -1;
    private long stored = 0;

    public SegmentedBuffer() {
    }

    public SegmentedBuffer(long sizeHint) {
        // handle small-ish buffers. Other hint values else will use default segments
        if (sizeHint >0 && sizeHint < SEGMENT_SIZE){
            addSegment(sizeHint);
        }
    }

    private void addSegment(long size){
        currentSegment = new byte[(int) size];
        posInSegment = 0;
        segments.add(currentSegment);
        segmentIndex = segments.size()-1;
    }

    private void addSegment(){
        addSegment(SEGMENT_SIZE);
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

        long posPastSegment = 0;
        segmentIndex = -1;
        for (byte[] seg: segments) {
            segmentIndex++;
            posPastSegment += seg.length;
            if (posPastSegment > pos){
                currentSegment = seg;
                posInSegment = (int)(pos - (posPastSegment - seg.length));
                break;
            }
        }
    }

    private byte[] getCurrentSegment(){
        if (currentSegment != null){
            return currentSegment;
        }

        if (segments.isEmpty()){
            addSegment();
            return currentSegment;
        }

        throw new IllegalStateException("currentSegment should be set");
    }

    private int availableInSegment(){
        return getCurrentSegment().length - posInSegment;
    }

    public int put(final byte[] b, final int off, final int len){
        if (availableInSegment() == 0){
            addSegment();
        }

        int written = Math.min(availableInSegment(), len);
        System.arraycopy(b, off, currentSegment, posInSegment, written);
        pos += written;
        posInSegment += written;
        stored += written;
        return written;
    }

    public int put(final byte[] b) {
        return put(b, 0, b.length);
    }

    public int get(final byte[] b, final int off, final int len){
        if (available() == 0){
            return -1;
        }

        int read = Math.min(availableInSegment(), len);
        System.arraycopy(currentSegment, posInSegment, b, off, read);
        pos += read;
        posInSegment += read;
        advanceSegmentIfNeeded();
        return read;
    }

    private void advanceSegmentIfNeeded() {
        if (posInSegment >= currentSegment.length){
            posInSegment = 0;
            segmentIndex++;
            currentSegment = segmentIndex < segments.size() ? segments.get(segmentIndex) : null;
        }
    }

    public int get(final byte[] b){
        return get(b, 0, b.length);
    }

    public int get(){
        if (available() == 0){
            return -1;
        }

        int result = currentSegment[posInSegment];
        pos++;
        posInSegment++;
        advanceSegmentIfNeeded();
        return result;
    }
}
