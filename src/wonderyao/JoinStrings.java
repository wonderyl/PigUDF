package wonderyao;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class JoinStrings extends EvalFunc<String> {
	@Override
	public String exec(Tuple input) throws IOException {
		Tuple tpl = (Tuple)input.get(0);
		String delimiter = (String)input.get(1);
		StringBuilder sb = new StringBuilder(tpl.get(0).toString());
		for(int i=1; i<tpl.size(); ++i){
			sb.append(delimiter);
			sb.append(tpl.get(i).toString());
		}
		return sb.toString();
	}
	
	@Override
	public Schema outputSchema(Schema input){
		try{
			if(input.getFields().size() != 2 ||
				input.getField(0).type != DataType.TUPLE ||
				input.getField(1).type != DataType.CHARARRAY)
				throw new RuntimeException("expect {cliqueN:tuple(), delimiter:chararray} ");
			Schema result = new Schema();
			result.add(new FieldSchema("key", DataType.CHARARRAY));
			return result;
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
