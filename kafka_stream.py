import uuid

# Function to retrieve random user data from an API
def get_data():
    import requests
    
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    return res

# Function to format retrieved data into desired format
def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())  # Generate a random UUID for the user
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    # Format address information
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    
    return data

# Function to stream formatted user data to Kafka
def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging
    
    # Initialize Kafka producer
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    curr_time = time.time()  # Record current time
    
    # Stream data until 1 minute has elapsed
    while True:
        if time.time() > curr_time + 60: # 1 minute
            break
        try:
            # Retrieve and format user data
            res = get_data()
            res = format_data(res)            
            # Send formatted data to Kafka topic 'users_created'
            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'error ocurred: {e}')
            continue
        
stream_data()
