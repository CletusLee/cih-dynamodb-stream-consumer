package lambda;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;
import org.json.JSONObject;

public class EmployeeStreamHandler implements
		RequestHandler<DynamodbEvent, Object> {
	@Override
	public Object handleRequest(DynamodbEvent input, Context context) {
		LambdaLogger logger = context.getLogger();
		KinesisProducer producer = retriveKinesisProducer();

		for (DynamodbStreamRecord r : input.getRecords()) {
			Employee employee = new Employee();
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
				case "id":
					logger.log("id: " + v.getS());
					employee.setId(v.getS());
					break;
				case "gender":
					logger.log("gender: " + v.getS());
					employee.setGender(v.getS());
					break;
				case "team":
					logger.log("team: " + v.getS());
					employee.setTeam(v.getS());
					break;
				case "height":
					logger.log("height: " + v.getN());
					employee.setHeight(Integer.valueOf(v.getN()));
					break;
				case "age":
					logger.log("age: getN" + v.getN());
					employee.setAge(Integer.valueOf(v.getN()));
					break;
				case "name":
					logger.log("Name: " + v.getS());
					employee.setName(v.getS());
					break;
				default:
					context.getLogger().log("Key " + k + " is unknown.");
				}

			}

			try {
				String employeeJson = new JSONObject(employee).toString();
				logger.log("ready to write data: " + employeeJson);
				List<Future<UserRecordResult>> putFutures = new LinkedList<Future<UserRecordResult>>();
				putFutures.add(producer.addUserRecord("employee-event", "employee-event",
						ByteBuffer.wrap(employeeJson.getBytes("UTF-8"))));

				for (Future<UserRecordResult> f : putFutures) {
					logger.log("evaluate the results from Kinesis");
					UserRecordResult result = f.get(); // this does block
					if (result.isSuccessful()) {
						logger.log("Put record into shard " +
								result.getShardId());
					} else {
						for (Attempt attempt : result.getAttempts()) {
							// Analyze and respond to the failure
							logger.log("failed when adding records to Kinesis with the error: " + attempt.getErrorMessage());
						}
					}
					logger.log("finished from Kinesis");
				}

				logger.log("successfully");
			} catch (UnsupportedEncodingException | InterruptedException | ExecutionException e) {
				logger.log("failed to produce events. message = " + e.getMessage());
				e.printStackTrace();
			}
		}


		return null;
	}

	KinesisProducer retriveKinesisProducer() {
		KinesisProducerConfiguration config = new KinesisProducerConfiguration();
		config.setRegion("us-east-1");
		config.setMaxConnections(1);
		config.setRequestTimeout(60000);
		config.setRecordMaxBufferedTime(1000);
		return new KinesisProducer(config);
	}

}