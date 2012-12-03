package wonderyao;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class CalcClassWeight extends EvalFunc<DataBag> {
	TupleFactory tupleFactory = TupleFactory.getInstance();
	BagFactory bagFactory = BagFactory.getInstance();
	
	@Override
	public DataBag exec(Tuple input) throws IOException {
		DataBag bag = (DataBag)input.get(0);
		HashMap<String, Double> clsCnt = new HashMap<String, Double>();
		Iterator<Tuple> it = bag.iterator();
		Double sum = new Double(0.0);
		while(it.hasNext()){
			Tuple item = (Tuple)it.next();
			String cls = (String)item.get(3);
			if(cls!=null && cls.length()>0){
				Double cur = clsCnt.get(cls);
				Double inc = (Double)item.get(2);
				if(cur!=null){
					clsCnt.put(cls, cur+inc);
				}
				else{
					clsCnt.put(cls, inc);
				}
				sum += inc;
			}
		}
		
		Set<Entry<String, Double>> clses = clsCnt.entrySet();
		Iterator<Entry<String, Double>> cit = clses.iterator();
		DataBag result = bagFactory.newDefaultBag();
		while(cit.hasNext()){
			Entry<String, Double> cls = cit.next();
			Tuple tpl = tupleFactory.newTuple(2);
			tpl.set(0, cls.getKey());
			tpl.set(1, cls.getValue()/sum);
			result.add(tpl);
		}
		
		return result;
	}
	
	@Override
	public Schema outputSchema(Schema input){
		try{
			if(input.getFields().size() != 1 ||
				input.getField(0).type != DataType.BAG){
				throw new RuntimeException("expect input {bag}");
			}
			Schema bag = input.getField(0).schema.getField(0).schema;
			if(bag.getFields().size()<4 ||
				bag.getField(0).type != DataType.CHARARRAY||
				bag.getField(1).type != DataType.CHARARRAY||
				bag.getField(2).type != DataType.DOUBLE||
				bag.getField(3).type != DataType.CHARARRAY){
				throw new RuntimeException("expect input {userid:chararray, " +
						"md:chararray, weight:double, cls:chararray}");
			}
			
			Schema result = new Schema();
			result.add(new FieldSchema("cls", DataType.CHARARRAY));
			result.add(new FieldSchema("weight", DataType.DOUBLE));
			return result;
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}