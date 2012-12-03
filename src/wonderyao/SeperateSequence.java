package wonderyao;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.UDFContext;

public class SeperateSequence extends EvalFunc<DataBag>{
	BagFactory bagFactory = BagFactory.getInstance();
	TupleFactory tupleFactory = TupleFactory.getInstance();
	HashSet<String> sepSet;
	String filename;

	public SeperateSequence(String file){
		this.filename = file;
		UDFContext context = UDFContext.getUDFContext();
		Properties udfprop = context.getUDFProperties(this.getClass());
		sepSet = (HashSet<String>)udfprop.get("icfaSeps");
	}
	
	@Override
	public DataBag exec(Tuple argvs) throws IOException {
		DataBag sequence = (DataBag)argvs.get(0);
		int fieldNum = (Integer)argvs.get(1);
		
		DataBag result = bagFactory.newDefaultBag();
		String mark = null;
		DataBag piece = null;
		Iterator<Tuple> it = (Iterator<Tuple>)sequence.iterator();
		while(it.hasNext()){
			Tuple tpl = (Tuple)it.next();
			if(fieldNum<0 || fieldNum>tpl.size())
				continue;
			String icfa = (String)tpl.get(fieldNum);
			if(sepSet.contains(icfa)){
				if(piece != null){
					Tuple pair = tupleFactory.newTuple(2);
					pair.set(0, mark);
					pair.set(1, piece);
					result.add(pair);
				}
				mark = icfa;
				piece = bagFactory.newDefaultBag();
				piece.add(tpl);
			}
			else if(piece != null){
				piece.add(tpl);
			}
		}
		if(piece != null){
			Tuple pair = tupleFactory.newTuple(2);
			pair.set(0, mark);
			pair.set(1, piece);
			result.add(pair);
		}
		return result;
	}
	
	private HashSet<String> loadIcfaSeps() throws IOException{
		HashSet<String> icfas = new HashSet<String>();
		BufferedReader fin = new BufferedReader(new FileReader(this.filename));
		String line;
		while((line=fin.readLine())!=null){
			icfas.add(line);
		}
		if(icfas.isEmpty()){
			throw new IOException("no icfa sep loaded");
		}
		return icfas;
	}
	
	@Override
	public Schema outputSchema(Schema argvs){
		try {
			HashSet<String> icfaseps = loadIcfaSeps();
			UDFContext context = UDFContext.getUDFContext();
			Properties udfprop = context.getUDFProperties(this.getClass());
			udfprop.put("icfaSeps", icfaseps);
			
			if(argvs.size()!=2){
				throw new RuntimeException("expect argvs sequance, icfaFieldNum");
			}
			//Schema sequence = argvs.getField(1).schema;
			if(argvs.getField(1).type != DataType.INTEGER){
				throw new RuntimeException("expect icfaFieldNum of int");
			}
			Schema result = new Schema();
			result.add(new FieldSchema("mark", DataType.CHARARRAY));
			result.add(argvs.getField(0));
			return result;
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}
}
