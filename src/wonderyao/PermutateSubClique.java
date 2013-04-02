package wonderyao;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class PermutateSubClique extends EvalFunc<DataBag> {
	private BagFactory bagFactory = BagFactory.getInstance();
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	
	private String generateSubCliqueKey(Tuple clique, String delimitor, 
			int escape) throws ExecException{
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for(int i=0; i<clique.size();++i){
			if(i!=escape){
				if(!first){
					sb.append(delimitor);
				}
				else{
					first = false;
				}
				sb.append(clique.get(i).toString());
			}
		}
		return sb.toString();
	}
	
	@Override
	public DataBag exec(Tuple input) throws IOException {
		Tuple clique = (Tuple)input.get(0);
		String delimiter = (String)input.get(1);
		DataBag bag = bagFactory.newDefaultBag();
		for(int i=0; i<clique.size(); ++i){
			Tuple tpl = tupleFactory.newTuple(1);
			tpl.set(0, generateSubCliqueKey(clique, delimiter, i));
			bag.add(tpl);
		}
		return bag;
	}
	@Override
	public Schema outputSchema(Schema input){
		try{
			if(input.size()!=2 ||
				input.getField(0).type!=DataType.TUPLE ||
				input.getField(1).type!=DataType.CHARARRAY)
				throw new RuntimeException(
						"expect input as (clique:tuple, delimiter:chararray)");
			Schema bag = new Schema();
			bag.add(new FieldSchema("key", DataType.CHARARRAY));
			Schema result = new Schema();
			result.add(new FieldSchema("subCliques", bag));
			return result;
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
