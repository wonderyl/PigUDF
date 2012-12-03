package wonderyao;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class SessionDedup extends EvalFunc<DataBag> {
	long mSessionExpireMs;
	BagFactory bagFactory = BagFactory.getInstance();
	
	public SessionDedup(String sessionExpireMs){
		mSessionExpireMs = Long.parseLong(sessionExpireMs);
	}

	@Override
	public DataBag exec(Tuple input) throws IOException {
		DataBag result = bagFactory.newDefaultBag();
		DataBag session = (DataBag)input.get(0);
		Iterator<Tuple> it = (Iterator<Tuple>)session.iterator();
		long lasttime = 0;
		String lastid = ""; 
		while(it.hasNext()){
			Tuple record= it.next();
			long t = (Long)record.get(1);
			String id = (String)record.get(2);
			if(t-lasttime>mSessionExpireMs || id.compareTo(lastid)!=0){
				result.add(record);
			}
			lasttime = t;
		}
		return result;
	}
	
	@Override
	public Schema outputSchema(Schema argvs){
		try{
			Schema bag = argvs.getField(0).schema;
			Schema input = bag.getField(0).schema;
			if(input.size()<3 ||
				input.getField(0).type != DataType.CHARARRAY ||
				input.getField(1).type != DataType.LONG ||
				input.getField(2).type != DataType.CHARARRAY){
				//String msg = String.format("%d, %d, %d, %s", 
				//		argvs.getField(0).type,
				//		bag.size(),
				//		bag.getField(0).type,
				//		bag.getField(0).alias);
				//throw new RuntimeException(msg);
				throw new RuntimeException("expect input {(uuid: chararray, t: long, id: chararray)}");
			}
			return bag;
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
