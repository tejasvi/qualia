<div align="center" id="top"><img src="https://gistcdn.githack.com/tejasvi/2c6a412988d061620f6cb878e7c7b6ce/raw/75e78fca74a40b617552cc6ec19bfbd0249e2804/qualia.svg?min=1" align="center" alt="qualia" width="9999"></div>
<br>
<div align="center" id="top"><i>An extensible graph based knowledge store.</i></div>

# Features

* Vim based plain-text file UI.
* Fuzzy search.
* No syntax.
* Multi-parent nodes.
* End-to-end encryption.
* Syncing with Git.
* Realtime sync using Firebase.
* Robust conflict resolution.
* Automated backups.

# Quick start

1. [Run in browser](https://mybinder.org/v2/gh/tejasvi/qualia/master)
    * Uses [recommended config](binder/binder_init.vim) for best experience.
    * It may take a couple of minutes to load if not used by someone recently.
    * After startup click on <kbd>Terminal</kbd> in the middle.

2. Install locally
    * Add `Plug 'tejasvi/qualia'` to Vim config and then restart.
    * Do `:PlugInstall|UpdateRemotePlugins`.
    * Run with `vi .q.md` or `:e .q.md`.
    * Shortcuts use `<Leader>` which is assumed to be `Space` in the readme.

# Usage

File name corresponds to a specific node. The node descendants are displayed in form of nested list under the current node's content.

```markdown
Current node content.
Can contain multiple lines.
* First child content.
  [Image](https://i.imgur.com/6O265V5.jpg)
* Second child content
    * Further descendants can be displayed.
      ```
      print("Second child's child")
      ```
```

<details>
    <summary> What is Vim?</summary>
  <blockquote>
  Vim is a text editor with a "normal" mode and an editing mode. Cursor movement and scrolling can be done with mouse.<br>
  <kbd>i</kbd> to start editing. <br>
  <kbd>Esc</kbd> to switch back to "normal" mode.<br>
  <kbd>Ctrl+o</kbd> to go to last location.<br>
  <kbd>yy</kbd> to copy current line.<br>
  <kbd>p</kbd> to paste below current line.<br>
  <kbd><<</kbd> and <kbd>>></kbd> to indent left and right.
  </blockquote>
</details>

Following shortcuts apply to the node under cursor.

|Action|Shortcut|
|-|-|
|Toggle descendants|<kbd>Space</kbd><kbd>j</kbd>|
|Hoist node|<kbd>Space</kbd><kbd>k</kbd>|
|Invert links ([transpose](https://en.wikipedia.org/wiki/Transpose_graph))|<kbd>Space</kbd><kbd>l</kbd>|

To add a child node, create a list item. To duplicate a node, copy and paste the node content lines. To unlink, delete the lines.
<details>
   <summary>How it works?</summary>
  <blockquote>Each node line contains a unique identifier concealed using Vim's <code>:h conceal</code>. To toggle identifier visibility press <kbd>co</kbd>.</blockquote>
</details>

# Trivia

* Files are used for displaying content instead of storing.
* File names are "pointers" to nodes and file path is irrelevant.<br>E.g. Node "a" can be opened with `vi a.q.md` from anywhere.
* <kbd>Space</kbd><kbd>p</kbd> to toggle parsing of buffer changes. Useful during advanced node manipulation.
* <kbd>Space</kbd><kbd>/</kbd> to fuzzy search nodes containing specific term and <kbd>Space</kbd><kbd>?</kbd> to fuzzy search all nodes.
* <kbd>Space</kbd><kbd>L</kbd> to open inverted connection graph and <kbd>Space</kbd><kbd>K</kbd> to hoist node in a new buffer.
* `:ListOrphans` to list unaccessible nodes not linked by others. `:RemoveOrphans` to delete orphan nodes.
* The node connections form directed cyclic graph with ordered vertices.
* See [config.py](rplugin/python3/qualia/config.py) for advanced configuration.
* Qualia is currently sparse on documentation due to planned decentralized community support.

<hr>
<details><summary><b> Comparison with existing tools</b></summary>
<h4>Vimflowy</h4>
<ul>
<li>Limited markdown syntax.</li>
<li>Node can not be simultaneously expanded and collapsed at different locations.</li>
<li>Ancestor node can not be added as a descendant.</li>
<li>Does not support multi-line content in a node.</li>
<li>Limited node graph manipulation capabilities.</li>
<li>No E2E encryption during sync.</li>
<li>Vim _emulation_ vs Vim.</li>

</ul>
<h4>Workflowy</h4>
<ul>

<li>Not markdown.</li>
<li>Not keyboard-centric.</li>
<li>Paid and closed-source.</li>
<li>Data loss possible during sync conflict resolution.</li>
<li>Limited node graph manipulation capabilities.</li>
<li>Multi-line notes are second-class.</li>
<li>No E2E encryption during sync.</li>

</ul>
<h4>Roam Research</h4>
<ul>

<li>Paid and closed-source.</li>
<li>Not markdown.</li>
<li>Not key-board centric.</li>
<li>Limited node graph manipulation capabilities.</li>
<li>Extremely slow UI.</li>
<li>No E2E encryption during sync.</li>
<li>Data loss possible during sync conflict resolution.</li>

</ul>
<h4>Obsidian</h4>
<ul>

<li>Closed-source.</li>
<li>Transclusion only in preview mode.</li>
<li>Not keyboard-centric.</li>
<li>UX suitable only for multi-line notes.</li>
<li>Limited node graph manipulation capabilities.</li>

</ul>
</details>
