package wonderyao;

import java.io.IOException;
import java.util.HashMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class ExtraUrlField extends EvalFunc<String> {
	@Override
	public String exec(Tuple input) throws IOException {
		String url = (String)input.get(0);
		if(url == null)
			return "";
		String key = (String)input.get(1);
		String[] cells = url.split("\\?", 2);
		if(cells.length>=2){
			String params = cells[1];
			String[] kvs = params.split("&");
			HashMap<String, String> hm = new HashMap<String, String>(2*kvs.length);
			for(String kv:kvs){
				String[] item = kv.split("=", 2);
				if(item.length==2){
					hm.put(item[0], item[1]);
				}
			}
			String value = hm.get(key);
			return value==null?"":value;
		}
		else{
			return "";
		}
	}
	
	@Override
	public Schema outputSchema(Schema input){
		try {
			if(input.getField(0).type != DataType.CHARARRAY){
				throw new RuntimeException("expect input Url:chararray");
			}
		} catch (FrontendException e) {
			e.printStackTrace();
		}
		Schema output = new Schema();
		output.add(new FieldSchema("", DataType.CHARARRAY));
		return output;
	}
}
