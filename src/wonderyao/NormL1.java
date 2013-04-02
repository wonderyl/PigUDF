package wonderyao;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class NormL1 extends EvalFunc<Double> {
	@Override
	public Double exec(Tuple input) throws IOException {
		Tuple vct = (Tuple)input.get(0);
		double norml1 = 0.0;
		for(int i=0; i<vct.size(); ++i){
			norml1 += Math.abs((Double)vct.get(i));
		}
		return norml1;
	}
	@Override
	public Schema outputSchema(Schema input){
		try{
			if(input.size()!=1 ||
					input.getField(0).type!=DataType.TUPLE){
				throw new RuntimeException("expect input as vector:Tuple");
			}
			Schema result = new Schema();
			result.add(new FieldSchema("normL1", DataType.DOUBLE));
			return result;
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
