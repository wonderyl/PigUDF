package wonderyao;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class CrossSelf extends EvalFunc<DataBag>{
	BagFactory bagFactory = BagFactory.getInstance();
	TupleFactory tupleFactory = TupleFactory.getInstance();
	
	private void copyTuple(final Tuple from, Tuple to){
		for(int i=0; i<from.size(); ++i){
			try {
				to.set(i, from.get(i));
			} catch (ExecException e) {
			}
		}
	}
	
	@Override
	public DataBag exec(Tuple input) throws IOException {
		DataBag result = bagFactory.newDefaultBag();
		DataBag bag = (DataBag)input.get(0);
		long size = bag.size();
		long index = 0;
		Iterator<Tuple> it = (Iterator<Tuple>)bag.iterator();
		while(it.hasNext()){
			Tuple record = (Tuple)it.next();
			for(long i=0; i<size; ++i){
				if(i!=index){
					Tuple newrecord = tupleFactory.newTuple(record.size()+1);
					copyTuple(record, newrecord);
					String crossKey;
					if(index<i)
						crossKey = String.format("%d,%d", index, i);
					else
						crossKey = String.format("%d,%d", i, index);
					newrecord.set(newrecord.size()-1, crossKey);
					result.add(newrecord);
				}
			}
			++index;
		}
		return result;
	}

	@Override
	public Schema outputSchema(Schema input){
		try{
			if(input.getField(0).type != DataType.BAG){
				throw new RuntimeException("expect input bag");
			}
			Schema bag = input.getField(0).schema.getField(0).schema.clone();
			bag.add(new FieldSchema("crossKey", DataType.CHARARRAY));
			return bag;
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
