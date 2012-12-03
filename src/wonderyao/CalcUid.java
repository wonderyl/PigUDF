package wonderyao;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class CalcUid extends EvalFunc<String> {
	TupleFactory tupleFactory = TupleFactory.getInstance();
	
	@Override
	public String exec(Tuple input) throws IOException {
		Long loginqq = (Long)input.get(0);
		String uuid = (String)input.get(1);
		Long mapqq = (Long)input.get(2);
		if(loginqq==null){
			if(mapqq==null){
				return "U"+uuid;
			}
			else{
				return String.format("Q%d", mapqq);
			}
		}
		else{
			if(mapqq==null){
				return String.format("Q%d", mapqq);
			}
			else{
				return String.format("Q%d", mapqq);
			}
		}
	}
	
	public Schema outputSchema(Schema input){
		try{
			if(input.getFields().size() != 3 ||
				input.getField(0).type != DataType.LONG ||
				input.getField(1).type != DataType.CHARARRAY ||
				input.getField(2).type != DataType.LONG){
				//throw new RuntimeException(String.format("%d, %d, %d, %d", input.getFields().size(), input.getField(0).type, input.getField(1).type, input.getField(2).type));
				throw new RuntimeException("expect input (login qq:long, uuid:chararray, map qq:long)");
			}
			Schema result = new Schema();
			result.add(new FieldSchema("uid", DataType.CHARARRAY));
			return result;
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
		
	}
}
