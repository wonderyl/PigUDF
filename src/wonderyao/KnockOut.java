package wonderyao;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class KnockOut extends EvalFunc<Tuple> {
	TupleFactory tupleFactory = TupleFactory.getInstance();
	
	class Pair{
		int index;
		double value;
	};
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		Tuple vector = (Tuple)input.get(0);
		int count = (Integer)input.get(1);
		Pair[] pairs = new Pair[vector.size()];
		int[] dict = new int[vector.size()];
		for(int i=0; i<vector.size(); ++i){
			pairs[i] = new Pair();
			pairs[i].index = i;
			pairs[i].value = (Double)vector.get(i);
			dict[i] = 1;
		}
		Arrays.sort(pairs, new Comparator<Pair>(){
			@Override
			public int compare(Pair a, Pair b) {
				return Double.compare(a.value, b.value);
			}
		});
		for(int i=0; i<count; ++i){
			dict[pairs[i].index] = 0;
		}
		Tuple result = tupleFactory.newTuple(vector.size());
		for(int i=0; i<vector.size(); ++i){
			if(dict[i]>0)
				result.set(i, vector.get(i));
			else
				result.set(i, 0.0);
		}
		return result;
	}
	@Override
	public Schema outputSchema(Schema input){
		try{
			if(input.getFields().size() != 2)
				throw new RuntimeException("expect input as (vector:tuple, count:int");
			Schema result = new Schema();
			result.add(new FieldSchema("tpl", input));
			return result;
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
