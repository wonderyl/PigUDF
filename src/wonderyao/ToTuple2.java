package wonderyao;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class ToTuple2 extends EvalFunc<Tuple>{
	//TupleFactory tupleFactory = TupleFactory.getInstance();
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		String item1 = (String)input.get(0);
		String item2 = (String)input.get(1);
		if(item1.compareTo(item2)<0)
			return input;
		else 
			return null;
	}
	@Override
	public Schema outputSchema(Schema input){
		try{
			if(input.getFields().size() != 2)
				throw new RuntimeException("expect input with 2 columns");
			Schema result = new Schema();
			result.add(new FieldSchema("tpl", input));
			return result;
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
