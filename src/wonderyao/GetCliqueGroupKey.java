package wonderyao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class GetCliqueGroupKey extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		Tuple clique = (Tuple)input.get(0);
		if(clique.size()>0){
			StringBuilder sb = new StringBuilder(clique.get(0).toString());
			for(int i=1; i<clique.size()-1; ++i){
				sb.append('\t');
				sb.append(clique.get(i).toString());
			}
			return sb.toString();
		}
		else{
			return null;
		}
	}

	@Override
	public Schema outputSchema(Schema input){
		try{
			if(input.getFields().size() != 1 ||
					input.getField(0).type != DataType.TUPLE)
				throw new RuntimeException("expect (clique:tuple())");
			//if(input.getField(0).schema.size()<2)
			//	throw new RuntimeException("expect clique size>=2");
			Schema result = new Schema();
			result.add(new FieldSchema("key", DataType.CHARARRAY));
			return result;
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
