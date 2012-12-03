package wonderyao;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class Limit extends EvalFunc<DataBag> {
	BagFactory bagFactory = BagFactory.getInstance();
	
	public DataBag exec(Tuple input) throws IOException {
		DataBag bag = (DataBag)input.get(0);
		int n = (Integer)input.get(1);
		
		DataBag result = bagFactory.newDefaultBag();
		Iterator<Tuple> it = bag.iterator();
		int count = 0;
		while(it.hasNext() && count<n){
			result.add(it.next());
			++count;
		}

		return result;
	}
	
	@Override
	public Schema outputSchema(Schema input){
		try{
			if(input.getFields().size() != 2 ||
				input.getField(0).type != DataType.BAG ||
				input.getField(1).type != DataType.INTEGER){
				throw new RuntimeException("expect input ({bag}, n: int)");
			}
			Schema bag = input.getField(0).schema;
			return bag;
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
