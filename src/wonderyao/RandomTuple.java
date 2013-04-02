package wonderyao;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class RandomTuple extends EvalFunc<Tuple> {
	TupleFactory tplFact = TupleFactory.getInstance();
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		int size = (Integer)input.get(0);
		double upperRange = (Double)input.get(1);
		Tuple result = tplFact.newTuple(size);
		for(int i=0; i<size; ++i){
			result.set(i, Math.random()*upperRange);
		}
		return result;
	}

	@Override
	public Schema outputSchema(Schema input){
		try {
			if(input.size()!=2 
					|| input.getField(0).type!=DataType.INTEGER
					|| input.getField(1).type!=DataType.DOUBLE){
				throw new RuntimeException("expect input as size:integer, upperRange:double");
			}
			Schema result = new Schema();
			result.add(new FieldSchema("result", DataType.TUPLE));
			return result;
		} catch (FrontendException e) {
			throw new RuntimeException(e);
		}
	}
}