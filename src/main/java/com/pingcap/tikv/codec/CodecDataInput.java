/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.codec;

import com.google.protobuf.ByteString;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;


public class CodecDataInput implements DataInput {
    private final DataInputStream       inputStream;
    private final ByteArrayInputStream  backingStream;
    private final byte[]                backingBuffer;

    public CodecDataInput(ByteString data) {
        this(data.toByteArray());
    }

    public CodecDataInput(byte[] buf) {
        backingBuffer = buf;
        backingStream = new ByteArrayInputStream(buf);
        inputStream = new DataInputStream(backingStream);
    }
    @Override
    public void readFully(byte[] b) {
        try {
            inputStream.readFully(b);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void readFully(byte[] b, int off, int len) {
        try {
            inputStream.readFully(b, off, len);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int skipBytes(int n) {
        try {
            return inputStream.skipBytes(n);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean readBoolean() {
        try {
            return inputStream.readBoolean();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte readByte() {
        try {
            return inputStream.readByte();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readUnsignedByte() {
        try {
            return inputStream.readUnsignedByte();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public short readShort() {
        try {
            return inputStream.readShort();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readUnsignedShort() {
        try {
            return inputStream.readUnsignedShort();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public char readChar() {
        try {
            return inputStream.readChar();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readInt() {
        try {
            return inputStream.readInt();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long readLong() {
        try {
            return inputStream.readLong();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public float readFloat() {
        try {
            return inputStream.readFloat();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public double readDouble() {
        try {
            return inputStream.readDouble();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String readLine() {
        try {
            return inputStream.readLine();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String readUTF() {
        try {
            return inputStream.readUTF();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean eof() {
        return backingStream.available() == 0;
    }

    public int size() { return backingStream.available();}

    public byte[] toByteArray() {
        return backingBuffer;
    }
}
