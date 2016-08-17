package ldbc.snb.bteronh.structures;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by aprat on 16/08/16.
 */
public class Edge implements WritableComparable<Edge> {

    private long tail;
    private long head;

    public Edge(long tail, long head) {
        this.tail = tail < head ? tail : head;
        this.head = head > tail ? head : tail;
    }

    public long getTail() {
        return tail;
    }

    public long getHead() {
        return head;
    }

    @Override
    public int compareTo(Edge o) {
        if(tail < o.tail) return -1;
        if(tail > o.tail) return 1;
        if(head < o.head) return -1;
        if(head > o.head) return 1;
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(tail);
        dataOutput.writeLong(head);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        tail = dataInput.readLong();
        head = dataInput.readLong();
    }
}
