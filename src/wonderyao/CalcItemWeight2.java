package wonderyao;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class CalcItemWeight2 extends EvalFunc<DataBag> {
	TupleFactory tupleFactory = TupleFactory.getInstance();
	BagFactory bagFactory = BagFactory.getInstance();
	
	@Override
	public DataBag exec(Tuple input) throws IOException {
		DataBag bag = (DataBag)input.get(0);
		//double total = (Double)input.get(1);
		if(bag.size()!=2){
			return null;
		}
		Iterator<Tuple> it = bag.iterator();
		Tuple item1 = (Tuple)it.next();
		Tuple item2 = (Tuple)it.next();
		double w1 =  (Double)item1.get(2);
		double w2 = (Double)item2.get(2);
		double sum = w1+w2;
		Double weight = w1*w2/sum/sum;
		
		Tuple result1 = tupleFactory.newTuple(3);
		result1.set(0, item1.get(1));
		result1.set(1, item2.get(1));
		result1.set(2, weight);
		Tuple result2 = tupleFactory.newTuple(3);
		result2.set(0, item2.get(1));
		result2.set(1, item1.get(1));
		result2.set(2, weight);
		DataBag result = bagFactory.newDefaultBag();
		result.add(result1);
		result.add(result2);
		return result;
	}
	
	@Override
	public Schema outputSchema(Schema input){
		try{
			if(input.getFields().size() != 1 ||
				input.getField(0).type != DataType.BAG ){
				throw new RuntimeException("expect input ({bag})");
			}
			Schema bag = input.getField(0).schema.getField(0).schema;
			if(bag.getFields().size()<3 ||
				bag.getField(0).type != DataType.CHARARRAY||
				bag.getField(1).type != DataType.CHARARRAY||
				bag.getField(2).type != DataType.DOUBLE){
				throw new RuntimeException("expect input ({uuid: chararray, id: chararray, count: double}, total: double)");
			}
			
			Schema result = new Schema();
			result.add(new FieldSchema("item1", DataType.CHARARRAY));
			result.add(new FieldSchema("item2", DataType.CHARARRAY));
			result.add(new FieldSchema("count", DataType.DOUBLE));
			return result;
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
