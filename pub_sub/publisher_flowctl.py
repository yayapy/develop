"""Publishes multiple messages to a Pub/Sub topic with an error handler."""
import datetime
import time
from concurrent import futures
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import (
    LimitExceededBehavior,
    PublisherOptions,
    PublishFlowControl,
)

# TODO(developer)
project_id = "data-cube-migration"
topic_id = "bucket_history"

# Configure how many messages the publisher client can hold in memory
# and what to do when messages exceed the limit.
flow_control_settings = PublishFlowControl(
    message_limit=100,  # 100 messages
    byte_limit=10 * 1024 * 1024,  # 10 MiB
    limit_exceeded_behavior=LimitExceededBehavior.BLOCK,
)
publisher = pubsub_v1.PublisherClient(
    publisher_options=PublisherOptions(flow_control=flow_control_settings)
)
topic_path = publisher.topic_path(project_id, topic_id)
publish_futures = []


# Resolve the publish future in a separate thread.
def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
    message_id = publish_future.result()
    # print(message_id)


# Publish 1000 messages in quick succession may be constrained by
# publisher flow control.
for n in range(1, 1000):
    data_str = f"Message number {n}"
    # Data must be a bytestring
    data = data_str.encode("utf-8")
    publish_future = publisher.publish(topic_path, data)
    # Non-blocking. Allow the publisher client to batch messages.
    publish_future.add_done_callback(callback)
    publish_futures.append(publish_future)

futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

print(f"Published messages with flow control settings to {topic_path}.")