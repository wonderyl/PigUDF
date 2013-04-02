package wonderyao;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class AorB extends EvalFunc<Object> {

	@Override
	public Object exec(Tuple input) throws IOException {
		Object a = input.get(0);
		Object b = input.get(1);
		if(a != null){
			return a;
		}
		else{
			return b;
		}
	}

	@Override
	public Schema outputSchema(Schema input){
		try {
			if(input.size()!=2){
				throw new RuntimeException("expect 2 inputs");
			}
			Schema result = new Schema();
			result.add(new FieldSchema("result", input.getField(0).schema));
			return result;
		} catch (FrontendException e) {
			throw new RuntimeException(e);
		}
	}
}
