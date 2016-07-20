package com.soteradefense.dga;

import com.soteradefense.dga.io.formats.DGAAbstractEdgeInputFormat;
import com.soteradefense.dga.io.formats.RawEdge;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.ReverseEdgeDuplicator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Created by ishizuka on 2016/07/11.
 */
public class SpecialFormat extends DGAAbstractEdgeInputFormat<LongWritable> {

    private static final String DEFAULT_EDGE_VALUE = "1";

    /**
     * The create edge reader first determines if we should reverse each edge; some data sets are undirected graphs
     * and need the input format to reverse their connected nature.
     *
     * Calls the abstract method getEdgeReader() which will give us the appropriate EdgeReader for the subclass.
     * @param split
     * @param context
     * @return
     * @throws IOException
     */
    public EdgeReader<Text, LongWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
        String duplicator = getConf().get(IO_EDGE_REVERSE_DUPLICATOR, IO_EDGE_REVERSE_DUPLICATOR_DEFAULT);
        boolean useDuplicator = Boolean.parseBoolean(duplicator);
        EdgeReader<Text, LongWritable> reader = useDuplicator ? new ReverseEdgeDuplicator<Text, LongWritable>(new SpecialFormat.SperialReader()) : new SpecialFormat.SperialReader();
        return reader;
    }

    public class SperialReader extends DGAAbstractEdgeReader<LongWritable> {

        protected String getDefaultEdgeValue() {
            return DEFAULT_EDGE_VALUE;
        }

        protected void validateEdgeValue(RawEdge edge) throws IOException {
            try {
                Long.parseLong(edge.getEdgeValue());
            } catch (Exception e) {
                throw new IOException("Unable to convert the edge value into a Long: " + edge.getEdgeValue(), e);
            }
        }

        @Override
        protected LongWritable getValue(RawEdge edge) {
            return new LongWritable(Long.parseLong(edge.getEdgeValue()));
        }

        @Override
        protected RawEdge preprocessLine(Text line) throws IOException {
            String[] values = line.toString().split("\t");
            RawEdge edge = new RawEdge(values[0], values[2], "1");
            validateEdgeValue(edge);
            return edge;
        }
    }

}

