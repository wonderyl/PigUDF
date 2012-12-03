package wonderyao;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class DateRange extends EvalFunc<String>{
	Date today;
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	public DateRange(String todayStr) throws ParseException{
		this.today = dateFormat.parse(todayStr);
	}
	
	@Override
	public String exec(Tuple input) throws IOException {
		String str = (String)input.get(0);
		try {
			Date access = dateFormat.parse(str);
			long diff = today.getTime() - access.getTime();
			long days = Math.round((double)diff/3600000/24);
			if(days==0){
				return "today";
			}
			else if(days<2){
				return "2 days";
			}
			else if(days<7){
				return "1 week";
			}
			else if(days<14){
				return "2 weeks";
			}
			else if(days<30){
				return "1 month";
			}
			else if(days<60){
				return "2 months";
			}
			else if(days<180){
				return "half year";
			}
			else if(days<365){
				return "1 year";
			}
			else{
				return "others";
			}
		} catch (ParseException e) {
			return "unknown";
		}
	}
	
	@Override
	public Schema outputSchema(Schema input){
		try{
			if(input.getField(0).type!=DataType.CHARARRAY){
				throw new RuntimeException("expect input as Chararray");
			}
			Schema output = new Schema();
			output.add(new FieldSchema("dateRange", DataType.CHARARRAY));
			return output;
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}
}
