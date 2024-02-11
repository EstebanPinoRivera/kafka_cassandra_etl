import logging
from cassandra.cluster import Cluster
from kafka import KafkaConsumer
import uuid

# Function to create the keyspace in Cassandra
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")

# Function to create the table in Cassandra
def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        dob TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)
    print("Table created successfully!")

# Function to insert data into Cassandra
def insert_data(session, user_data):
    print("inserting data...")
    try:
        # Convert id to UUID object
        user_data['id'] = uuid.UUID(user_data['id'])
        
        session.execute("""
            INSERT INTO streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_data['id'], user_data['first_name'], user_data['last_name'], user_data['gender'], user_data['address'],
              str(user_data['post_code']), user_data['email'], user_data['username'], user_data['dob'], user_data['registered_date'], user_data['phone'], user_data['picture']))
        logging.info("Data inserted for {} {}".format(user_data['first_name'], user_data['last_name']))

    except Exception as e:
        logging.error('Could not insert data due to {}'.format(e))

# Function to create a connection to Cassandra
def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error("Could not create cassandra connection due to {}".format(e))
        return None

if __name__ == "__main__":
    # Create a connection to Cassandra
    session = create_cassandra_connection()

    if session is not None:
        # Create keyspace and table if they don't exist
        create_keyspace(session)
        create_table(session)
        
        # Kafka consumer configuration
        consumer = KafkaConsumer('users_created',
                                 bootstrap_servers='localhost:9092',
                                 group_id='my-group',
                                 auto_offset_reset='earliest')

        # Start consuming messages from Kafka topic
        for message in consumer:
            data = message.value.decode('utf-8')  # Assuming data is in JSON format
            user_data = eval(data)  # Assuming data is a JSON string
            insert_data(session, user_data)
