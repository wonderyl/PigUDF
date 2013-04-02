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

/*
 * lost function = sigma( (r_ij-p_i * q_j)^2) + lambda1*(|p_i| + |q_j|) 
 * 								+ lambda2*(Entropy(p_i)+Entropy(q_j) ); 
 * Entropy(p)= -|p|^2+|p| 
 */

public class MatrixFactorizationGradianDescend extends EvalFunc<Tuple> {
	TupleFactory tplFact = TupleFactory.getInstance();
	private Double learningRate, lambda1, lambda2;
	
	public MatrixFactorizationGradianDescend(String learningRateStr, 
			String lambda1Str, String lambda2Str){
		this.learningRate = Double.parseDouble(learningRateStr);
		this.lambda1 = Double.parseDouble(lambda1Str);
		this.lambda2 = Double.parseDouble(lambda2Str);
	}
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		Tuple vector = (Tuple)input.get(0);
		DataBag e_ui = (DataBag)input.get(1);
		Integer oppositeIndex = (Integer)input.get(2);
		Tuple result = tplFact.newTuple(vector.size());
		double sum = 0.0;
		for(int i=0; i<result.size(); ++i){
			Double oldValue = (Double)vector.get(i);
			sum += Math.abs(oldValue);
		}
		for(int i=0; i<result.size(); ++i){
			Double oldValue = (Double)vector.get(i);
			Double value = new Double(oldValue);
			
			double p = Math.abs(oldValue)/sum;
			 
			if(oldValue>0){
				//l1 norm
				value -= learningRate*lambda1;
				//entropy
				value += learningRate*lambda2*(2*p-1);
			}
			else{
				value += learningRate*lambda1;
				value += learningRate*lambda2*(2*p+1);
			}
			result.set(i, value);
		}
		
		Iterator<Tuple> it = e_ui.iterator();

		double[] delta = new double[result.size()];
		int totalErr = 0;
		while(it.hasNext()){
			Tuple record = it.next();
			Double error = (Double)record.get(4);
			++totalErr;
			Tuple opposite = (Tuple)record.get(oppositeIndex);
			for(int i=0; i<result.size(); ++i){
				delta[i] += learningRate*error*(Double)opposite.get(i);
			}
		}
		if(totalErr != 0){
			for(int i=0; i<result.size(); ++i){
				result.set(i, (Double)result.get(i)+delta[i]/totalErr);
			}
		}
		return result;
	}

	@Override
	public Schema outputSchema(Schema input){
		try {
			if(input.size()!=3 
					|| input.getField(0).type!=DataType.TUPLE
					|| input.getField(1).type!=DataType.BAG
					|| input.getField(2).type!=DataType.INTEGER){
				throw new RuntimeException("expect input as (vector:tuple, "+
						"e_ui:bag, oppositeIndex:int)");
			}
			Schema result = new Schema();
			result.add(new FieldSchema("result", DataType.TUPLE));
			return result;
		} catch (FrontendException e) {
			throw new RuntimeException(e);
		}
	}
}
