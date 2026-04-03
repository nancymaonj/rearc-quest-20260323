def handler(event, context):
    for record in event["Records"]:
        print("Bucket:", record["s3"]["bucket"]["name"])
        print("Object:", record["s3"]["object"]["key"])
        print("Event:", record["eventName"])