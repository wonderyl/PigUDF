package wonderyao;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

public class NovelCFStore extends StoreFunc {
	RecordWriter writer = null;
	@Override
	public OutputFormat getOutputFormat() throws IOException {
		return new TextOutputFormat<LongWritable, Text>();
	}

	@Override
	public void prepareToWrite(RecordWriter writer) throws IOException {
		this.writer = writer;
	}

	@Override
	public void putNext(Tuple input) throws IOException {
		String item = (String)input.get(0);
		DataBag bag = (DataBag)input.get(1);
		Iterator<Tuple> it = bag.iterator();
		StringBuilder sb = new StringBuilder();
		sb.append(item);
		sb.append('\t');
		while(it.hasNext()){
			Tuple record = (Tuple)it.next();
			String recom = (String)record.get(1);
			sb.append(recom);
			sb.append("\t");
		}
		try{
			writer.write(null, new Text(sb.toString()));
		}
		catch(InterruptedException ie){
			throw new IOException(ie);
		}
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		FileOutputFormat.setOutputPath(job, new Path(location));
	}
	
	public void checkSchema(ResourceSchema input) throws IOException {
		if(input.getFields().length != 2 ||
				input.getFields()[0].getType()!=DataType.CHARARRAY ||
				input.getFields()[1].getType()!=DataType.BAG){
			throw new RuntimeException("need input as {item1: chararray, {BAG} }");
		}
		ResourceSchema bag = input.getFields()[1].getSchema().getFields()[0].getSchema();
		if(bag.getFields().length != 3 ||
			bag.getFields()[0].getType() != DataType.CHARARRAY ||
			bag.getFields()[1].getType() != DataType.CHARARRAY ||
			bag.getFields()[2].getType() != DataType.DOUBLE){
			throw new RuntimeException("need input as {item1: chararray, {item1: chararray, item2: chararray, weight: double} }");
		}
	}

}
