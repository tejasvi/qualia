from __future__ import annotations

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

app = pyrebase.initialize_app(pyrebase_config)
live = LiveData(app, '/')


# data = live.get_data()
# sub_data = data.get('test')
# print("test", data, sub_data)


def my_handler(sender, value, path):
    print("signal", sender, value, path)


live.signal('/').connect(my_handler)
while True:
    sleep(0.5)

exit()

"""
r
• flskdfjslkd
    • hahalslskdfj laskdjflskdfjkk <DEL>
        ‣ hahalslskdfj laskdjflskdfjkk
        • hunl
    • s
        • flskdfjslkd
            ‣ hahalslskdfj laskdjflskdfjkk
            • s <s not detected as only child of parent to turn into ordered node len(children_ids/context > 1)>
                ‣ flskdfjslkd
• hahalslskdfj laskdjflskdfjkk
    ‣ hahalslskdfj laskdjflskdfjkk
    • hunl
"""
a = (
    ['r',
     '- [](q://AXscAISsO2XA76YLPREb4A==)  flskdfjslkd',
     '        + [](q://AXscD4GbNBsRZE7dzbDcKw==)  hahalslskdfj [laskdjf](hlsdjf)lskdfjkk',
     '        - [](q://AXscEFdpN-rYaoHtv7yEHA==)  hunl',
     '    1. [](q://AXscAKhyY7PmgFrSPY4n1A==)  s',
     '    1. [](q://AXscAISsO2XA76YLPREb4A==)  flskdfjslkd',
     '             + [](q://AXscD4GbNBsRZE7dzbDcKw==)  hahalslskdfj [laskdjf](hlsdjf)lskdfjkk',
     '             - [](q://AXscEFdpN-rYaoHtv7yEHA==)  hunl',
     '        - [](q://AXscAKhyY7PmgFrSPY4n1A==)  s',
     '            + [](q://AXscAISsO2XA76YLPREb4A==)  flskdfjslkd',
     '                    + [](q://AXscD4GbNBsRZE7dzbDcKw==)  hahalslskdfj [laskdjf](hlsdjf)lskdfjkk',
     '                    - [](q://AXscEFdpN-rYaoHtv7yEHA==)  hunl',
     '- [](q://AXscD4GbNBsRZE7dzbDcKw==)  hahalslskdfj [laskdjf](hlsdjf)lskdfjkk',
     '    + [](q://AXscD4GbNBsRZE7dzbDcKw==)  hahalslskdfj [laskdjf](hlsdjf)lskdfjkk',
     '    - [](q://AXscEFdpN-rYaoHtv7yEHA==)  hunl'],
    ['r',
     '- [](q://AXscAISsO2XA76YLPREb4A==)  flskdfjslkd',
     '        + [](q://AXscD4GbNBsRZE7dzbDcKw==)  hahalslskdfj [laskdjf](hlsdjf)lskdfjkk',
     '        - [](q://AXscEFdpN-rYaoHtv7yEHA==)  hunl',
     '    1. [](q://AXscAKhyY7PmgFrSPY4n1A==)  s',
     '    1. [](q://AXscAISsO2XA76YLPREb4A==)  flskdfjslkd',
     '             + [](q://AXscD4GbNBsRZE7dzbDcKw==)  hahalslskdfj [laskdjf](hlsdjf)lskdfjkk',
     '             - [](q://AXscEFdpN-rYaoHtv7yEHA==)  hunl',
     '    1. [](q://AXscAKhyY7PmgFrSPY4n1A==)  s',
     '        + [](q://AXscAISsO2XA76YLPREb4A==)  flskdfjslkd',
     '                + [](q://AXscD4GbNBsRZE7dzbDcKw==)  hahalslskdfj [laskdjf](hlsdjf)lskdfjkk',
     '                - [](q://AXscEFdpN-rYaoHtv7yEHA==)  hunl',
     '- [](q://AXscD4GbNBsRZE7dzbDcKw==)  hahalslskdfj [laskdjf](hlsdjf)lskdfjkk',
     '    + [](q://AXscD4GbNBsRZE7dzbDcKw==)  hahalslskdfj [laskdjf](hlsdjf)lskdfjkk',
     '    - [](q://AXscEFdpN-rYaoHtv7yEHA==)  hunl']
)
exit()
from time import time

from ntplib import NTPClient

a = time()
offset = NTPClient().request('pool.ntp.org')

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
