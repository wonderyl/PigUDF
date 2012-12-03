package wonderyao;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class IsPositive extends EvalFunc<Integer> {

	@Override
	public Integer exec(Tuple input) throws IOException {
		long value = (Long)input.get(0);
		if(value>0){
			return 1;
		}
		else{
			return 0;
		}
	}
	
	@Override
	public Schema outputSchema(Schema input){
		try{
			if(input.getField(0).type!=DataType.LONG){
				throw new RuntimeException("expect input as Long");
			}
			Schema output = new Schema();
			output.add(new FieldSchema("isPositive", DataType.INTEGER));
			return output;
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
