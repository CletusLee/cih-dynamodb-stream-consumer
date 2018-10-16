package lambda;

import java.util.Map.Entry;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;

public class EmployeeStreamHandler implements
		RequestHandler<DynamodbEvent, Object> {
	@Override
	public Object handleRequest(DynamodbEvent input, Context context) {
		LambdaLogger logger = context.getLogger();

		for (DynamodbStreamRecord r : input.getRecords()) {

			StreamRecord sr = r.getDynamodb();
			for (Entry<String, AttributeValue> entry : sr.getNewImage()
					.entrySet()) {
				String k = entry.getKey();
				AttributeValue v = entry.getValue();
				switch (k) {
				case "employeeEventType":
					logger.log("Event Type: " + v.getS());
					break;
				case "time":
					logger.log("Time: " + v.getS());
					break;
				case "name":
					logger.log("Name: " + v.getS());
					break;
				default:
					context.getLogger().log("Key " + k + " is unknown.");
				}
			}
		}


		return null;
	}

}