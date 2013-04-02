package wonderyao;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class GenerateCliqueCandidate extends EvalFunc<DataBag> {
	TupleFactory tupleFactory = TupleFactory.getInstance();
	BagFactory bagFactory = BagFactory.getInstance();
	
	@Override
	public DataBag exec(Tuple input) throws IOException {
		DataBag result = bagFactory.newDefaultBag();
		DataBag bag = (DataBag)input.get(0);
		LinkedList<Tuple> list = new LinkedList<Tuple>();
		Iterator<Tuple> it = bag.iterator();
		while(it.hasNext()){
			Tuple keyClique = it.next();
			if(keyClique.size()!=2)
				continue;
			Tuple clique1 = (Tuple)keyClique.get(1);
			Iterator<Tuple> jt = list.iterator();
			while(jt.hasNext()){
				Tuple clique2 = jt.next();
				Tuple merged = tupleFactory.newTuple(clique1.size()+1);
				for(int i=0; i<clique1.size()-1; ++i){
					merged.set(i, clique1.get(i));
				}
				String a = clique1.get(clique1.size()-1).toString();
				String b = clique2.get(clique2.size()-1).toString();
				if(a.compareTo(b)<0){
					merged.set(clique1.size()-1, clique1.get(clique1.size()-1));
					merged.set(clique1.size(), clique2.get(clique2.size()-1));
				}
				else{
					merged.set(clique1.size()-1, clique2.get(clique2.size()-1));
					merged.set(clique1.size(), clique1.get(clique1.size()-1));
				}
				StringBuilder sb = new StringBuilder(merged.get(1).toString());
				for(int i=2; i<merged.size(); ++i){
					sb.append('\t');
					sb.append(merged.get(i).toString());
				}
				Tuple tpl = tupleFactory.newTuple(2);
				tpl.set(0, merged);
				tpl.set(1, sb.toString());
				result.add(tpl);
			}
			list.add(clique1);
		}
		return result;
	}
	@Override
	public Schema outputSchema(Schema input){
		try{
			if(input.getFields().size() != 1 ||
				input.getField(0).type != DataType.BAG)
				throw new RuntimeException("expect (Bag)");
			Schema bag = input.getField(0).schema.getField(0).schema;
			if(bag.size()!=2 || 
				bag.getField(0).type!=DataType.CHARARRAY ||
				bag.getField(1).type!=DataType.TUPLE)
				throw new RuntimeException(String.format(
						"expect {key:chararray, clique:tuple} %d, %d, %d", 
						bag.size(), bag.getField(0).type, bag.getField(1).type));
			Schema tuple = new Schema();
			tuple.add(new FieldSchema("cliqueNPlus1", DataType.TUPLE));
			tuple.add(new FieldSchema("key", DataType.CHARARRAY));
			Schema result = new Schema();
			result.add(new FieldSchema("tpl", tuple));
			return result;
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
