package wonderyao;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/* base on assumption:
 * p_u_k*sum(q_i_k) = sum(r_u_i)/K
*/
public class InitItem extends EvalFunc<Tuple>{
	TupleFactory tplFact = TupleFactory.getInstance();
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		int K = (Integer)input.get(0);
		DataBag combine = (DataBag)input.get(1);
		Iterator<Tuple> it = combine.iterator();
		double[] sumInterest = new double[K];
		for(int i=0; i<K; ++i){
			sumInterest[i] = 0.0;
		}
		double sumRating = 0.0;
		while(it.hasNext()){
			Tuple record = it.next();
			sumRating += (Double)record.get(2);
			Tuple interest = (Tuple)record.get(4);
			for(int i=0; i<K; ++i){
				sumInterest[i] += (Double)interest.get(i);
			}
		}
		Tuple result = tplFact.newTuple(K);
		sumRating /= K;
		for(int i=0; i<K; ++i){
			result.set(i, sumRating/sumInterest[i]);
		}
		return result;
	}
	
	@Override
	public Schema outputSchema(Schema input){
		try {
			if(input.size()!=2 
					|| input.getField(0).type!=DataType.INTEGER
					|| input.getField(1).type!=DataType.BAG){
				throw new RuntimeException("expect input as (dimension:int, combine:bag)");
			}
			Schema result = new Schema();
			result.add(new FieldSchema("result", DataType.TUPLE));
			return result;
		} catch (FrontendException e) {
			throw new RuntimeException(e);
		}
	}
}
