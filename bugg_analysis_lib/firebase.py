import os
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud import storage


@firestore.transactional
def _mark_complete_in_transaction(
    transaction, analysis_id: str, audio_id: str, detections: list, bf
):
    """
    Marks the analysis as complete in the database.

    Placed outside the class in order to use firestore.transactional decorator,
    which doesn't seem to play well with class methods
    """
    audioRef = bf.db.collection("audio").document(audio_id)

    snapshot = audioRef.get(transaction=transaction)
    analysesPerformed = snapshot.get("analysesPerformed")

    if analysis_id in analysesPerformed:
        print(f"WARNING: Analysis {analysis_id} was already completed for {audio_id}")
    else:
        analysesPerformed.append(analysis_id)

    audioRecord = snapshot.to_dict()
    newDetectionsList = []

    # Add in the old detections and merge any of the old records
    # We merge because the analysis may be re-run after an update
    if "detections" in audioRecord:
        prevDetections = snapshot.get("detections")
        for d in prevDetections:
            match = next((x for x in detections if d["id"] == x["id"]), None)
            print(f"match {match}")
            if match is None:
                newDetectionsList.append(d)
            else:
                # Merge the two, letting the newer one overrite the old
                newDetectionsList.append({**d, **match})

    for d in detections:
        match = next((x for x in newDetectionsList if d["id"] == x["id"]), None)
        if match is None:
            newDetectionsList.append(d)

    transaction.update(
        audioRef,
        {
            "analysesPerformed": analysesPerformed,
            "detections": newDetectionsList,
            "hasDetections": len(newDetectionsList) > 0,
        },
    )


class BuggFirebase:
    "A class to handle Firestore and Cloud Storage interactions"

    def __init__(self, project_id: str):
        if not firebase_admin._apps:
            cred = credentials.ApplicationDefault()
            firebase_admin.initialize_app(
                cred,
                {
                    "projectId": project_id,
                },
            )
        self.db = firestore.client()

    def get_audio_db_record(self, audio_id: str):
        # fetches the record we have for this audio file from the database
        audio_ref = self.db.collection("audio").document(audio_id)
        doc = audio_ref.get()
        if doc.exists:
            return doc.to_dict()

        return None

    def get_analysis_result(self, analysis_id: str, audio_id: str):
        """
        Will return the result from firestore if there is one or None if not.

        Note that storing results in the palce is optional (and subject to 1mb limit). This method is just for convenience
        """
        results_ref = (
            self.db.collection("audio")
            .document(audio_id)
            .collection(analysis_id)
            .document("result")
        )
        doc = results_ref.get()
        if doc.exists:
            return doc.to_dict()

        return None

    def set_analysis_result(self, analysis_id: str, audio_id: str, result: dict):
        """
        Store a result in the common place in firestore. Will merge into any existing result.

        (Storing in this spot is optional.)

        Note no transactions are used, you may want to explore them if writing multiple entries.
        """
        results_ref = (
            self.db.collection("audio")
            .document(audio_id)
            .collection(analysis_id)
            .document("result")
        )
        results_ref.set(result, merge=True)

    def mark_analysis_complete(self, analysis_id: str, audio_id: str, detections: list):
        """
        Updates the audio record to show that analysis is done (which will kick off other analyses)
        """
        transaction = self.db.transaction()
        _mark_complete_in_transaction(
            transaction, analysis_id, audio_id, detections, self
        )

    def download_audio(self, analysis_id: str, audio_record: dict) -> str:
        """
        Downloads the audio file from cloud storage to a place locally.

        Be sure to delete the file after processing.

        Will return the filename once download is complete
        """

        audio_id = audio_record["id"]
        uri = audio_record["uri"]

        destinationPath = f"tmp/{analysis_id}"
        Path(destinationPath).mkdir(parents=True, exist_ok=True)
        destinationFile = f"{destinationPath}/{audio_id}.mp3"

        print(f"Downloading {uri}")

        client = storage.Client()
        with open(destinationFile, "wb") as file_obj:
            client.download_blob_to_file(uri, file_obj)

        return destinationFile

    def delete_downloaded_audio(self, filepath: str):
        os.remove(filepath)
