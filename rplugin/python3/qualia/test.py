from markdown_it import MarkdownIt
from markdown_it.tree import SyntaxTreeNode
from mdit_py_plugins import tasklists

print(tasklists)

from qualia.models import NodeId

k = '\n'.join(
    ['r', '- [](q://AXrTVCs7cfrCb-CF07mSpw)  f', '      * ha      ', '    - [](q://AXrTVR8ZL4u0xeZq8frhMA)  ha      ',
     '    + [](q://AXrTVCs7cfrCb-CF07mSpw)  f', '          * ha      ']) + '\n'
s = """r
1. f
    1. s
"""
print(s)

ast = SyntaxTreeNode(MarkdownIt().parse(s))

exit()

from buffer import Process

text = """* Intersting stuff
lskdjflsdjf
* laksdjfoin
* iaosn"""

x = Process().process_lines(text.splitlines(), NodeId("testrootid"))

print(x)
