package com.app.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
public class Element implements Writable {
    int tag;
    int index;
    double value;

    Element() {
        tag = 0;
        index = 0;
        value = 0.0;
    }

    Element(int tag, int index, double value) {
        this.tag = tag;
        this.index = index;
        this.value = value;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        tag = input.readInt();
        index = input.readInt();
        value = input.readDouble();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(tag);
        output.writeInt(index);
        output.writeDouble(value);
    }
}