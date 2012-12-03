package wonderyao;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class IndexSourceLoader extends LoadFunc{
	protected class KVField {
		public String name;
		public byte type;
	}
	static protected HashMap<String, Byte> dataTypeTable = new HashMap<String, Byte>();
	static {
		dataTypeTable.put("CHARARRAY", DataType.CHARARRAY);
		dataTypeTable.put("BYTEARRAY", DataType.BYTEARRAY);
		dataTypeTable.put("INTEGER", DataType.INTEGER);
		dataTypeTable.put("INT", DataType.INTEGER);
		dataTypeTable.put("LONG", DataType.LONG);
		dataTypeTable.put("BYTE", DataType.BYTE);
		dataTypeTable.put("BOOLEAN", DataType.BOOLEAN);
		dataTypeTable.put("DOUBLE", DataType.DOUBLE);
		dataTypeTable.put("FLOAT", DataType.FLOAT);
	}
	protected RecordReader reader = null;
	protected KVField[] fields = null;
	protected final Log log = LogFactory.getLog(getClass());
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	
	private byte dataTypeFromString(String str){
		String ustr = str.toUpperCase();
		Byte v = dataTypeTable.get(ustr);
		if(v != null){
			return v.byteValue();
		}
		else{
			return DataType.UNKNOWN;
		}
	}
	
	public IndexSourceLoader(String fieldsStr){
		String[] fieldStrs = fieldsStr.split(",");
    	this.fields = new KVField[fieldStrs.length];
    	for(int i=0; i<fieldStrs.length; ++i){
    		fieldStrs[i] = fieldStrs[i].trim();
    		String[] cells = fieldStrs[i].split(":", 2);
    		this.fields[i] = new KVField();
    		if(cells.length==1){
    			this.fields[i].name = cells[0].trim();
    			this.fields[i].type = DataType.UNKNOWN;
    		}
    		else{
    			this.fields[i].name = cells[0].trim();
    			this.fields[i].type = dataTypeFromString(cells[1].trim());
    		}
    	}
	}
	
	@Override
	public InputFormat getInputFormat() throws IOException {
		return new TextInputFormat();
	}
	
	private Object nomalizeValue(String value, byte type){
		switch(type){
		case DataType.CHARARRAY:
			return value;
		case DataType.BYTEARRAY:
			return value;
		case DataType.INTEGER:
			try{
				return Integer.parseInt(value);
			}
			catch(Exception e){
				return 0;
			}
		case DataType.LONG:
			try{
				return Long.parseLong(value);
			}
			catch(Exception e){
				return 0L;
			}
		case DataType.BYTE:
			try{
				return Byte.parseByte(value);
			}
			catch(Exception e){
				return 0;
			}
		case DataType.BOOLEAN:
			try{
				return Boolean.parseBoolean(value);
			}
			catch(Exception e){
				return false;
			}
		case DataType.DOUBLE:
			try{
				return Double.parseDouble(value);
			}
			catch(Exception e){
				return 0.0;
			}
		case DataType.FLOAT:
			try{
				return Float.parseFloat(value);
			}
			catch(Exception e){
				return 0.0F;
			}
		default:
			return value;
		}
	}
	
	@Override
	public Tuple getNext() throws IOException {
		HashMap<String, String> kvmap = new HashMap<String, String>();
		try{
			Text val = null;
			String lastKey = null;
			while(true){
				if(reader.nextKeyValue()){
					val = (Text)reader.getCurrentValue();
					//log.info(val.toString());
					if(val.getLength()<2){
						;
					}
					else if(val.charAt(0)=='!' && val.charAt(1)=='!'){
						if(kvmap.size()>0){
							break;
						}
					}
					else if(val.charAt(0)==' ' && val.charAt(1)==' '){
						if(lastKey!=null){
							String line = new String(val.getBytes(), 0, val.getLength(), "GBK");
							kvmap.put(lastKey, kvmap.get(lastKey)+line.substring(2));
						}
					}
					else{
						String line = new String(val.getBytes(), 0, val.getLength(), "GBK");
						lastKey = line.substring(0,2);
						kvmap.put(lastKey, line.substring(2));
						//log.info(line.substring(0,2));
						//log.info(line.substring(2));
					}
				}
				else{
					if(kvmap.size()==0)
						return null;
					else
						break;
				}
			}
			if(kvmap.size()>0){
				Tuple t = tupleFactory.newTuple(this.fields.length);
				for(int i=0; i<this.fields.length; ++i){
					String value = kvmap.get(fields[i].name);
					//log.info(value);
					t.set(i, nomalizeValue(value, fields[i].type));
					//log.info(nomalizeValue(value, fields[i].type));
				}
				return t;
			}
			else{
				return null;
			}
		}catch(InterruptedException ie){
			throw new IOException(ie);
		}
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		this.reader = reader;
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location);
	}
}
