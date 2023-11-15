import os
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud import storage

# TODO: make this a class so that DB instance is not global and always initialised

db = None


def initFirebase(project_id):
    global db
    if not firebase_admin._apps:
        cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(
            cred,
            {
                "projectId": project_id,
            },
        )
    db = firestore.client()


def getAudioDBRecord(audioId: str):
    # fetches the record we have for this audio file from the database
    audio_ref = db.collection("audio").document(audioId)
    doc = audio_ref.get()
    if doc.exists:
        return doc.to_dict()

    return None


def getAnalysisResult(analysisId: str, audioId: str):
    """
    Will return the result from firestore if there is one or None if not.

    Note that storing results in the palce is optional (and subject to 1mb limit). This method is just for convenience
    """
    results_ref = (
        db.collection("audio")
        .document(audioId)
        .collection(analysisId)
        .document("result")
    )
    doc = results_ref.get()
    if doc.exists:
        return doc.to_dict()

    return None


def setAnalysisResult(analysisId: str, audioId: str, result: dict):
    """
    Store a result in the common place in firestore. Will merge into any existing result.

    (Storing in this spot is optional.)

    Note no transactions are used, you may want to explore them if writing multiple entries.
    """
    results_ref = (
        db.collection("audio")
        .document(audioId)
        .collection(analysisId)
        .document("result")
    )
    results_ref.set(result, merge=True)


def markAnalysisComplete(analysisId: str, audioId: str, detections: list):
    """
    Updates the audio record to show that analysis is done (which will kick off other analyses)
    """
    transaction = db.transaction()
    _markCompleteInTransaction(transaction, analysisId, audioId, detections)


@firestore.transactional
def _markCompleteInTransaction(
    transaction, analysisId: str, audioId: str, detections: list
):
    audioRef = db.collection("audio").document(audioId)

    snapshot = audioRef.get(transaction=transaction)
    analysesPerformed = snapshot.get("analysesPerformed")

    if analysisId in analysesPerformed:
        print(f"WARNING: Analysis {analysisId} was already completed for {audioId}")
    else:
        analysesPerformed.append(analysisId)

    audioRecord = snapshot.to_dict()
    newDetectionsList = []

    # Add in the old detections and merge any of the old records
    # We merge because the analysis may be re-run after an update
    if "detections" in audioRecord:
        prevDetections = snapshot.get("detections")
        for d in prevDetections:
            match = next((x for x in detections if d["id"] == x["id"]), None)
            print(f"match {match}")
            if match == None:
                newDetectionsList.append(d)
            else:
                # Merge the two, letting the newer one overrite the old
                newDetectionsList.append({**d, **match})

    for d in detections:
        match = next((x for x in newDetectionsList if d["id"] == x["id"]), None)
        if match == None:
            newDetectionsList.append(d)

    transaction.update(
        audioRef,
        {
            "analysesPerformed": analysesPerformed,
            "detections": newDetectionsList,
            "hasDetections": len(newDetectionsList) > 0,
        },
    )


def downloadAudio(analysisId: str, audioRecord: dict) -> str:
    """
    Downloads the audio file from cloud storage to a place locally.

    Be sure to delete the file after processing.

    Will return the filename once download is complete
    """

    audio_id = audioRecord["id"]
    uri = audioRecord["uri"]

    destinationPath = f"tmp/{analysisId}"
    Path(destinationPath).mkdir(parents=True, exist_ok=True)
    destinationFile = f"{destinationPath}/{audio_id}.mp3"

    print(f"Downloading {uri}")

    client = storage.Client()
    with open(destinationFile, "wb") as file_obj:
        client.download_blob_to_file(uri, file_obj)

    return destinationFile


def deleteDownloadedAudio(filepath: str):
    os.remove(filepath)
