from concurrent.futures import TimeoutError

from google.cloud import pubsub_v1

from bugg_analysis_lib.firebase import BuggFirebase


# The function formerly known as start()
def init_subscription(subscription_id, analysis_id, on_message_cb):
    "Create a subscription to a Pub/Sub topic with given callback"
    project_id = "bugg-301712"
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    # Limit the subscriber to only have two outstanding messages at a time.
    flow_control = pubsub_v1.types.FlowControl(max_messages=1)

    streaming_pull_future = subscriber.subscribe(
        subscription_path, callback=on_message_cb, flow_control=flow_control
    )
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result()
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.


class BuggMessageAnalyser:
    """
    A class to handle the analysis of a single message from Pub/Sub
    """

    def __init__(self, message, analysis_id: str, fb: BuggFirebase):
        self.message = message
        self.audio_file_path = None
        self.analysis_id = analysis_id
        self.fb = fb

    def __enter__(self):
        # Decode the audio ID
        self.audio_id = self.message.data.decode("utf-8")

        # Fetch the database record
        self.audio_rec = self.fb.get_audio_db_record(self.audio_id)
        if self.audio_rec is None:
            print(f"Audio record not found for {self.audio_id}")
            raise ValueError("Audio record not found")

        # Download the audio file
        self.audio_file_path = self.fb.download_audio(self.analysis_id, self.audio_rec)
        return self

    def submit_detections(self, detections):
        # Add the detections to the database
        self.fb.mark_analysis_complete(
            analysis_id=self.analysis_id, audio_id=self.audio_id, detections=detections
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Cleanup: always delete the audio file and ack the message
        # TODO: consider whether there are situations when we shouldn't ack
        if self.audio_file_path:
            self.fb.delete_downloaded_audio(self.audio_file_path)
        self.message.ack()
