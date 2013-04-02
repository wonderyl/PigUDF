package wonderyao;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class ZeroIsNull extends EvalFunc<Integer> {
	@Override
	public Integer exec(Tuple input) throws IOException {
		Integer v = (Integer)input.get(0);
		if(v!=0)
			return v;
		else 
			return null;
	}
	@Override
	public Schema outputSchema(Schema input){
		try{
			Schema result = new Schema();
			result.add(new FieldSchema("result", DataType.INTEGER));
			return result;
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
