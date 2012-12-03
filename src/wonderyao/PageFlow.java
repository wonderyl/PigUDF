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

public class PageFlow extends EvalFunc<DataBag>{
	long mSessionExpireMs;
	
	BagFactory bagFactory = BagFactory.getInstance();
	TupleFactory tupleFactory = TupleFactory.getInstance();
	
	public PageFlow(String sessionExpireMs){
		mSessionExpireMs = Long.parseLong(sessionExpireMs);
	}

	@Override
	public DataBag exec(Tuple input) throws IOException {
		DataBag result = bagFactory.newDefaultBag();
		DataBag session = (DataBag)input.get(0);
		Iterator<Tuple> it = (Iterator<Tuple>)session.iterator();

		String lastChannel = "";
		int lastGF = 0;
		int lastLogin = 0;
		String lastPt = "";
		//String lastUUID = "";
		long lastT = 0;
		String lastURI = "";
		
		while(it.hasNext()){
			Tuple record= it.next();
			long time = (Long)record.get(5);
			if(time - lastT < mSessionExpireMs){
				Tuple tpl = tupleFactory.newTuple(8);
				tpl.set(0, lastChannel);
				tpl.set(1, lastGF);
				tpl.set(2, lastLogin);
				tpl.set(3, lastPt);
				tpl.set(4, lastChannel);
				tpl.set(5, lastURI);
				tpl.set(6, record.get(0));
				tpl.set(7, record.get(6));
				result.add(tpl);
			}
			else{
				if(lastT != 0){
					Tuple tpl = tupleFactory.newTuple(8);
					tpl.set(0, lastChannel);
					tpl.set(1, lastGF);
					tpl.set(2, lastLogin);
					tpl.set(3, lastPt);
					tpl.set(4, lastChannel);
					tpl.set(5, lastURI);
					tpl.set(6, "");
					tpl.set(7, "");
					result.add(tpl);
				}
				Tuple tpl = tupleFactory.newTuple(8);
				tpl.set(0, record.get(0));
				tpl.set(1, record.get(1));
				tpl.set(2, record.get(2));
				tpl.set(3, record.get(3));
				tpl.set(4, "");
				tpl.set(5, "");
				tpl.set(6, record.get(0));
				tpl.set(7, record.get(6));
				result.add(tpl);
			}
			lastChannel = (String)record.get(0);
			lastGF = (Integer)record.get(1);
			lastLogin = (Integer)record.get(2);
			lastPt = (String)record.get(3);
			//lastUUID = (String)record.get(4);
			lastT = (Long)record.get(5);
			lastURI = (String)record.get(6);
		}
		return result;
	}
	
	@Override
	public Schema outputSchema(Schema argvs){
		try{
			Schema bag = argvs.getField(0).schema;
			Schema input = bag.getField(0).schema;
			if(input.size()<7 ||
				input.getField(0).type != DataType.CHARARRAY ||
				input.getField(1).type != DataType.INTEGER ||
				input.getField(2).type != DataType.INTEGER ||
				input.getField(3).type != DataType.CHARARRAY ||
				input.getField(4).type != DataType.CHARARRAY ||
				input.getField(5).type != DataType.LONG ||
				input.getField(6).type != DataType.CHARARRAY){
				throw new RuntimeException("expect input {(channel:chararray ,g_f:int , login:int, pt:chararray, uuid:chararray, t:long, uri: chararray)}");
			}
			Schema tpl = new Schema();
			tpl.add(new FieldSchema("channel", DataType.CHARARRAY));
			tpl.add(new FieldSchema("g_f", DataType.INTEGER));
			tpl.add(new FieldSchema("login", DataType.INTEGER));
			tpl.add(new FieldSchema("pt", DataType.CHARARRAY));
			tpl.add(new FieldSchema("fromChannel", DataType.CHARARRAY));
			tpl.add(new FieldSchema("fromUri", DataType.CHARARRAY));
			tpl.add(new FieldSchema("toChannel", DataType.CHARARRAY));
			tpl.add(new FieldSchema("toUri", DataType.CHARARRAY));
			FieldSchema rtnbag = new FieldSchema("pageFlow", DataType.BAG);
			rtnbag.schema = tpl;
			return new Schema(rtnbag);
			
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
