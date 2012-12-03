package wonderyao;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class PermutateFields  extends EvalFunc<DataBag> {
	TupleFactory tupleFactory = TupleFactory.getInstance();
	BagFactory bagFactory = BagFactory.getInstance();
	String[] fieldNames;
	
	public PermutateFields(String fieldNameStr){
		this.fieldNames = fieldNameStr.split(",");
		for(int i=0; i< this.fieldNames.length; ++i){
			this.fieldNames[i] = this.fieldNames[i].trim();
		}
	}
	
	@Override
	public DataBag exec(Tuple input) throws IOException {
		DataBag bag = bagFactory.newDefaultBag();
		Tuple fields  = input;
		int n = (int)Math.pow(2, fields.size());
		for(int i=0; i<n; ++i){
			String key = new String();
			String value = new String();
			for(int k=0; k<fields.size(); ++k){
				int bit = i & (1<<k);
				if(bit != 0){
					key += fieldNames[k];
					key += "||";
					value += fields.get(k).toString();
					value += "||";
				}
			}
			if(key.length()>2){
				key = key.substring(0, key.length()-2);
				value = value.substring(0, value.length()-2);
			}
			Tuple tuple = tupleFactory.newTuple(2);
			tuple.set(0, key);
			tuple.set(1, value);
			bag.add(tuple);
		}
		return bag;
	}
	
	@Override
	public Schema outputSchema(Schema input){
		try{
			if(input.size()!=this.fieldNames.length){
				throw new RuntimeException("field size is not compatible with field names");
			}
			Schema tuple = new Schema();
			tuple.add(new FieldSchema("permutateKey", DataType.CHARARRAY));
			tuple.add(new FieldSchema("permutateValue", DataType.CHARARRAY));
			Schema bag = new Schema();
			FieldSchema fs = new FieldSchema("permutate", DataType.TUPLE);
			fs.schema = tuple;
			bag.add(fs);
			return bag;
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
