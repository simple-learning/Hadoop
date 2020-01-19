package com.app.hadoop;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Pair implements WritableComparable<Pair> {

    int i;
    int j;

    Pair() {
        i = 0;
        j = 0;
    }

    Pair(int i, int j) {
        this.i = i;
        this.j = j;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        i = input.readInt();
        j = input.readInt();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(i);
        output.writeInt(j);
    }

    @Override
    public int compareTo(Pair compare) {

        if (i > compare.i) {
            return 1;
        } else if (i < compare.i) {
            return -1;
        } else {
            if (j > compare.j) {
                return 1;
            } else if (j < compare.j) {
                return -1;
            }
        }
        return 0;
    }

    public String toString() {
        return i + " " + j + " ";
    }

}