from time import sleep

import pyrebase
from firebasedata import LiveData

pyrebase_config = {
    "apiKey": "AIzaSyDFNIazv7K0qDDJriiYPbhmB3OzUJYJvMI",
    "authDomain": "qualia-321013.firebaseapp.com",
    "databaseURL": "https://qualia-321013-default-rtdb.firebaseio.com",
    "projectId": "qualia-321013",
    "storageBucket": "qualia-321013.appspot.com",
    "messagingSenderId": "707949243379",
    "appId": "1:707949243379:web:db239176c6738dc5578086",
    "measurementId": "G-BPNP22GS5X"
}

print("app")
app = pyrebase.initialize_app(pyrebase_config)
print("live")
live = LiveData(app, '/')
print("data")

data = live.get_data()
sub_data = data.get('test')
print("test", data, sub_data)


def my_handler(sender, value, path):
    print("signal", sender, value, path)


live.signal('/').connect(my_handler)
while True:
    sleep(0.5)

exit()
from markdown_it import MarkdownIt
from markdown_it.tree import SyntaxTreeNode
from mdit_py_plugins import tasklists

print(tasklists)

k = '\n'.join(
    ['r', '- [](q://AXrTVCs7cfrCb-CF07mSpw)  f', '      * ha      ', '    - [](q://AXrTVR8ZL4u0xeZq8frhMA)  ha      ',
     '    + [](q://AXrTVCs7cfrCb-CF07mSpw)  f', '          * ha      ']) + '\n'
s = """r
* abc
- def
+ lol
"""
print(s)

ast = SyntaxTreeNode(MarkdownIt().parse(s))

exit()

text = """* Intersting stuff
lskdjflsdjf
* laksdjfoin
* iaosn"""
