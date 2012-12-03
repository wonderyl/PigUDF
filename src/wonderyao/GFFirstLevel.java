package wonderyao;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.UDFContext;

public class GFFirstLevel extends EvalFunc<String>{
	BagFactory bagFactory = BagFactory.getInstance();
	TupleFactory tupleFactory = TupleFactory.getInstance();
	HashMap<String, String> mapping;
	String filename;
	
	public GFFirstLevel(String file){
		this.filename = file;
		UDFContext context = UDFContext.getUDFContext();
		Properties udfprop = context.getUDFProperties(this.getClass());
		mapping = (HashMap<String, String>)udfprop.get("mapping");
	}

	@Override
	public String exec(Tuple input) throws IOException {
		String str = input.get(0).toString();
		if(str.compareTo("0") != 0){
			String value = mapping.get(str);
			if(value!=null){
				return value;
			}
			else{

				return "other";
			}
		}
		else{
			return "self";
		}
	}
	
	private HashMap<String, String> loadMapping() throws IOException{
		HashMap<String, String> tmp = new HashMap<String, String>();
		BufferedReader fin = new BufferedReader(new FileReader(this.filename));
		String line;
		while((line=fin.readLine())!=null){
			String[] cells = line.split("\t");
			if(cells.length==2){
				tmp.put(cells[0], cells[1]);
			}
		}
		if(tmp.isEmpty()){
			throw new IOException("no gf first level mapping loaded");
		}
		return tmp;
	}
	
	@Override
	public Schema outputSchema(Schema argvs){
		try {
			HashMap<String, String> tmp = loadMapping();
			UDFContext context = UDFContext.getUDFContext();
			Properties udfprop = context.getUDFProperties(this.getClass());
			udfprop.put("mapping", tmp);
			
			if(argvs.size()!=1){
				throw new RuntimeException("expect argvs g_f");
			}

			Schema result = new Schema();
			result.add(new FieldSchema("gfl1", DataType.CHARARRAY));
			return result;
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}
	
}
