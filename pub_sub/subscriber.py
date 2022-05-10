import time
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.oauth2 import service_account

# TODO(developer)
project_id = "data-cube-migration"
topic_id = "bucket_history"
subscription_id = "bucket_history-sub"
# Number of seconds the subscriber should listen for messages
timeout = 5.0

gcp_cred = service_account.Credentials.from_service_account_file('/Users/xfguo/workspace/.ssh/cloud_function_default.json')
subscriber = pubsub_v1.SubscriberClient(credentials=gcp_cred)
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)

msgs = []


def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    # print(f"Received {message}.")

    print(f"Received {message.data!r}.")
    # if message.attributes:
    #     # print("Attributes:")
    #     for key in message.attributes:
    #         value = message.attributes.get(key)
    #         print(f"{key}: {value}")

    # msgs.append(message)
    message.ack()


# streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
# print(f"Listening for messages on {subscription_path}..\n")
#
# # Wrap subscriber in a 'with' block to automatically call close() when done.
# with subscriber:
#     try:
#         # When `timeout` is not set, result() will block indefinitely,
#         # unless an exception is encountered first.
#         streaming_pull_future.result(timeout=timeout)
#     except TimeoutError:
#         streaming_pull_future.cancel()  # Trigger the shutdown.
#         streaming_pull_future.result()  # Block until the shutdown is complete.
#
# def callback(message: pubsub_v1.subscriber.message.Message) -> None:
#     print(f"Received {message.data!r}.")
#     if message.attributes:
#         print("Attributes:")
#         for key in message.attributes:
#             value = message.attributes.get(key)
#             print(f"{key}: {value}")
#     message.ack()

# Limit the subscriber to only have ten outstanding messages at a time.
flow_control = pubsub_v1.types.FlowControl(max_messages=10)
streaming_pull_future = subscriber.subscribe(subscription_path,
                                             callback=callback,
                                             flow_control=flow_control)

print(f"Listening for messages on {subscription_path}..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout)
        # streaming_pull_future.result()

    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.

print('pause......')
time.sleep(60)
print(f"Done messages")
# for m in msgs:
#     print(m.data)
#     print(m.attributes)
#     print(m.ordering_key)