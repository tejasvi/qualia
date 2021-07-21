from markdown_it import MarkdownIt
from markdown_it.tree import SyntaxTreeNode
from mdit_py_plugins import tasklists

print(tasklists)

from qualia.models import NodeId

k = '\n'.join(['alskjf', 'klj', '* [](q://AXrIYpjeBYGLc41OlkaA6g)  f', '    + [](q://AXrIYpjeBYGLc41OlkaA6g)  f',
               '    * [](q://AXrIYvEv1aZGXpQzWxNFiA)  I had a dream  ']) + '\n'
s = """kk
* abc
def
"""
print(s)

ast = SyntaxTreeNode(MarkdownIt().parse(k))

exit()

from buffer import Process

text = """* Intersting stuff
lskdjflsdjf
* laksdjfoin
* iaosn"""

x = Process().process_lines(text.splitlines(), NodeId("testrootid"))

print(x)
