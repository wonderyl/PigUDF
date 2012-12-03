package wonderyao;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class Bag2Map extends EvalFunc<Map<String, Object>>{
	public Map<String, Object> exec(Tuple input) throws IOException {
		DataBag bag = (DataBag)input.get(0);
		Map<String, Object> map = new HashMap<String, Object>();
		Iterator<Tuple> it = bag.iterator();
		while(it.hasNext()){
			Tuple item = (Tuple)it.next();
			map.put((String)item.get(0), item.get(1));
		}
		return map;
	}
	
	public Schema outputSchema(Schema input){
		try{
			if(input.getFields().size() != 1 ||
				input.getField(0).type != DataType.BAG){
				throw new RuntimeException("expect input {bag}");
			}
			Schema bag = input.getField(0).schema.getField(0).schema;
			if(bag.getFields().size()!=2 ||
				bag.getField(0).type != DataType.CHARARRAY){
				throw new RuntimeException("expect input {key:chararray, value}");
			}
			
			Schema result = new Schema();
			result.add(new FieldSchema("key", DataType.CHARARRAY));
			result.add(new FieldSchema("value", bag.getField(1).type));
			return result;
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
