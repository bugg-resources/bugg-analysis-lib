import hashlib
from concurrent.futures import TimeoutError

from google.cloud import pubsub_v1

from analyze import analyseAudioFile
from bugg import *

subscription_id = "analyses.birdnetlite-sub"
analysis_id = "birdnet-lite"


def on_process_audio(audio_id: str, audio_rec: dict, audio_file_path: str):
    
    print(f"PROCESSING audioId={audio_id}")
    uploadTime = audio_rec["uploadedAt"]
    week = uploadTime.isocalendar()[1]
    location = audio_rec["location"]

    results = analyseAudioFile(audio_file_path, week, location.latitude, location.longitude, 0.45)

    detections = []
    for r in results: 
        start, end, scientific_name, common_name, confidence = r
        
        # create the detections
        detections.append({
            u"id": hashlib.md5(f"{start}-{end}-{scientific_name}".encode('utf-8')).hexdigest()[:6],
            u"start": start,
            u"end": end,
            u"tags": [common_name],
            u"analysisId": analysis_id,
            u"confidence": confidence
        })

    # add the detections to the audio database record
    markAnalysisComplete(analysisId=analysis_id, audioId=audio_id, detections=detections)

    print(f"{audio_id} completed with {len(detections)} detections")


def on_message(message):
    audio_id = message.data.decode("utf-8") 

    # Fetch the database record we have for this audio clip
    audio_rec = getAudioDBRecord(audio_id)

    # Download the audio file 
    audio_file_path = downloadAudio(analysis_id, audio_rec)    

    # Run the birdnet processing
    on_process_audio(audio_id, audio_rec, audio_file_path)

    # Cleanup
    deleteDownloadedAudio(audio_file_path)

    message.ack()
    print(f"Processing complete for {audio_id}")


def start():
    project_id = "bugg-301712"
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    # Limit the subscriber to only have two outstanding messages at a time.
    flow_control = pubsub_v1.types.FlowControl(max_messages=1)

    streaming_pull_future = subscriber.subscribe(
        subscription_path, callback=on_message, flow_control=flow_control
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


if __name__ == "__main__":
    start()
