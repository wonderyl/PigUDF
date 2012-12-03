package wonderyao;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class Sequence16 extends EvalFunc<String> {
	private int current = 0;
	@Override
	public String exec(Tuple input) throws IOException {
		return String.format("%08x", current++);
	}
	
	@Override
	public Schema outputSchema(Schema input){
		Schema output = new Schema();
		output.add(new FieldSchema("sequence16", DataType.CHARARRAY));
		return output;
	}
}
