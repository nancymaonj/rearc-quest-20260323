from datetime import datetime
import quest_part_3_3 # type: ignore


def lambda_handler(event, context):
    now = datetime.utcnow().isoformat()

    for record in event["Records"]:
        print("Message:", record["body"])
        print("Timestamp:", now)
        print(quest_part_3_3.main()) 

    return {"timestamp": now}