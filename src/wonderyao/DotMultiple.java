package wonderyao;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class DotMultiple extends EvalFunc<Double> {
	
	@Override
	public Double exec(Tuple input) throws IOException {
		Tuple vector1 = (Tuple)input.get(0);
		Tuple vector2 = (Tuple)input.get(1);
		int size = vector1.size();
		
		Double result = 0.0;
		for(int i=0; i<size; ++i){
			result += (Double)vector1.get(i)*(Double)vector2.get(i);
		}
		return result;
	}

	@Override
	public Schema outputSchema(Schema input){
		try {
			if(input.size()!=2 
					|| input.getField(0).type!=DataType.TUPLE
					|| input.getField(1).type!=DataType.TUPLE){
				throw new RuntimeException("expect input as (vector1:tuple, "+
						"vector2:tuple) and vector1.size==vector2.size");
			}
			Schema result = new Schema();
			result.add(new FieldSchema("result", DataType.DOUBLE));
			return result;
		} catch (FrontendException e) {
			throw new RuntimeException(e);
		}
	}
}
