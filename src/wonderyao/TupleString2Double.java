package wonderyao;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class TupleString2Double extends EvalFunc<Tuple> {
	TupleFactory tplFact = TupleFactory.getInstance();
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		Tuple src = (Tuple)input.get(0);
		Tuple result = tplFact.newTuple(src.size());
		for(int i=0; i<result.size(); ++i){
			Double f = Double.parseDouble(src.get(i).toString());
			result.set(i, f);
		}
		return result;
	}
	@Override
	public Schema outputSchema(Schema input){
		try{
			if(input.size()!=1 ||
				input.getField(0).type!=DataType.TUPLE){
				throw new RuntimeException("expect input as tuple()");
			}
			Schema result = new Schema();
			result.add(new FieldSchema("result", DataType.TUPLE));
			return result;
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
