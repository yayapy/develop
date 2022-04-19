"""Publishes multiple messages to a Pub/Sub topic with an error handler."""
import datetime
import time
from concurrent import futures
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from google import api_core
# TODO(developer)
project_id = "data-cube-migration"
topic_id = "bucket_history"

gcp_cred = service_account.Credentials.from_service_account_file('/Users/xfguo/workspace/.ssh/cloud_function_default.json')

publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
batch_settings = pubsub_v1.types.BatchSettings(
    max_messages=10,  # default 100
    max_bytes=1024,  # default 1 MB
    max_latency=1,  # default 10 ms
)

publisher = pubsub_v1.PublisherClient(credentials=gcp_cred,
                                      publisher_options=publisher_options,
                                      batch_settings=batch_settings)
topic_path = publisher.topic_path(project_id, topic_id)
publish_futures = []

custom_retry = api_core.retry.Retry(
    initial=0.250,  # seconds (default: 0.1)
    maximum=90.0,  # seconds (default: 60.0)
    multiplier=1.45,  # default: 1.3
    deadline=300.0,  # seconds (default: 60.0)
    predicate=api_core.retry.if_exception_type(
        api_core.exceptions.Aborted,
        api_core.exceptions.DeadlineExceeded,
        api_core.exceptions.InternalServerError,
        api_core.exceptions.ResourceExhausted,
        api_core.exceptions.ServiceUnavailable,
        api_core.exceptions.Unknown,
        api_core.exceptions.Cancelled,
    ),
)


def get_callback(publish_future: pubsub_v1.publisher.futures.Future, data: str): # -> Callable[[pubsub_v1.publisher.futures.Future], None]:

    def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
        try:
            # Wait 60 seconds for the publish call to succeed.
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            print(f"Publishing {data} timed out.")

    return callback


# for i in range(10):
#     data = str(i)
#     # When you publish a message, the client returns a future.
#     publish_future = publisher.publish(topic_path, data.encode("utf-8"))
#     # Non-blocking. Publish failures are handled in the callback function.
#     publish_future.add_done_callback(get_callback(publish_future, data))
#     publish_futures.append(publish_future)
#
# # Wait for all the publish futures to resolve before exiting.
# futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
#
# print(f"Published messages with error handler to {topic_path}.")

# for n in range(1, 100):
#     data_str = f"Message number {n}"
#     # Data must be a bytestring
#     data = data_str.encode("utf-8")
#     # Add two attributes, origin and username, to the message
#     future = publisher.publish(
#         topic_path, data, origin="python-sample", username="gcp", retry=custom_retry
#     )
#     print(f'result: {future.result()}')
#
# print(f"Published messages with custom attributes to {topic_path}.")

# for message in [
#     ("message1", str(datetime.datetime.now()).encode('UTF-8')),
#     ("message2", str(datetime.datetime.now()).encode('UTF-8')),
#     ("message3", str(datetime.datetime.now()).encode('UTF-8')),
#     ("message4", str(datetime.datetime.now()).encode('UTF-8')),
# ]:
count = 0
while count <= 103:
    ts = str(datetime.datetime.now())
    num = '0000' + str(count)
    message = (f"{num[-4:]} message: {ts}", ts)
    # Data must be a bytestring
    data = message[0].encode("utf-8")
    ordering_key = message[1]
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, data=data,
                               ordering_key=ordering_key,
                               format='String', order_by='datetime')
    count += 1
    try:
        # print(future.result())
        print(f'\rpublished {count} message(s), last result is {future.result()}', end='')
    except:
        publisher.resume_publish(topic_path, ordering_key)
        print(f"Resumed publishing messages with ordering keys to {topic_path}.")

    time.sleep(1)

print(f"Published messages with ordering keys to {topic_path}.")
