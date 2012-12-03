package wonderyao;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class ExtraChapterNo extends EvalFunc<Integer> {
	@Override
	public Integer exec(Tuple input) throws IOException {
		try{
			String cmdStr = (String)input.get(0);
			if(cmdStr == null || cmdStr.length()==0)
				return -1;
			
			BigInteger cmd = new BigInteger(cmdStr);
			
			BigInteger rlt = cmd.and(BigInteger.valueOf((1<<14)-1));
			return rlt.intValue();
		}
		catch(Exception e){
			return -1;
		}
	}
	
	@Override
	public Schema outputSchema(Schema input){
		try {
			if(input.getField(0).type != DataType.CHARARRAY){
				throw new RuntimeException("expect input cmd:chararray");
			}
		} catch (FrontendException e) {
			e.printStackTrace();
		}
		Schema output = new Schema();
		output.add(new FieldSchema("cno", DataType.INTEGER));
		return output;
	}
}

