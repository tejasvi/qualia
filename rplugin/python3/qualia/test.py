from os import environ

import firebase_admin
from firebase_admin import db
from firebase_admin.db import Event

FIREBASE_WEB_APP_CONFIG = {
    # On https://console.firebase.google.com (free plan),
    # Go to Project Settings -> Add app -> "</>" (web app option)
    # Set name -> Continue -> Use the displayed "firebaseConfig"
    "apiKey": "AIzaSyDFNIazv7K0qDDJriiYPbhmB3OzUJYJvMI",
    "authDomain": "qualia-321013.firebaseapp.com",
    "databaseURL": "https://qualia-321013-default-rtdb.firebaseio.com",
    "projectId": "qualia-321013",
    "storageBucket": "qualia-321013.appspot.com",
    "messagingSenderId": "707949243379",
    "appId": "1:707949243379:web:db239176c6738dc5578086",
    "measurementId": "G-BPNP22GS5X"
}

environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Tejasvi\IdeaProjects\qualia\rplugin\python3\firebase-adminsdk.json"
# environ["FIREBASE_CONFIG"] = dumps(FIREBASE_WEB_APP_CONFIG)
default_app = firebase_admin.initialize_app(options=FIREBASE_WEB_APP_CONFIG)
ref = db.reference('/data', default_app)
res = ref.get()


def f(event: Event):
    print(event.data, event.path, event.event_type)


ref.listen(f)

pass
