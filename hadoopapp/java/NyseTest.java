import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.example.MapperData;
import com.example.ReducerData;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class NyseTest {
    MapDriver<LongWritable, Text, Text, FloatWritable> mapDriver;
    ReduceDriver<Text, FloatWritable, Text, FloatWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable> mapReduceDriver;

    @Before
    public void setUp() {
        MapperData mapper = new MapperData();
        ReducerData reducer = new ReducerData();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        //NYSE,AEA,2/8/2010,4.42,4.42,4.21,4.24,205500,4.24
        mapDriver.withInput(new LongWritable(), new Text("NYSE,AEA,2/8/2010,4.42,4.42,4.21,4.24,205500,4.24"));
        mapDriver.withOutput(new Text("AEA"), new FloatWritable(4.24f));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<FloatWritable> values = new ArrayList<FloatWritable>();
        values.add(new FloatWritable(1));
        values.add(new FloatWritable(1));
        reduceDriver.withInput(new Text("AEA"), values);
        reduceDriver.withOutput(new Text("AEA"), new FloatWritable(4.24f));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text("NYSE,AEA,2/8/2010,4.42,4.42,4.21,4.24,205500,4.24"));
        List<FloatWritable> values = new ArrayList<>();
        values.add(new FloatWritable(4.24f));
        mapReduceDriver.withOutput(new Text("AEA"), new FloatWritable(4.24f));
        mapReduceDriver.runTest();
    }
}
