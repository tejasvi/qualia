from os import getcwd

from dulwich import porcelain

x = porcelain.ls_remote("'https://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@github.com/tejasvi8874/qualia")
pass
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
