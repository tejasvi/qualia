call plug#begin(stdpath('data').'/plugged')
  Plug 'overcache/NeoSolarized'
  Plug 'tejasvi/vim-markdown'
  Plug 'junegunn/fzf', { 'do': { -> fzf#install() } }
  Plug 'tejasvi8874/qualia', { 'do': ':QualiaInstall', 'branch': 'dev'}
call plug#end()

hi CursorLine  cterm=NONE ctermbg=darkred ctermfg=white guibg=darkred guifg=white

let mapleader = ' '
set laststatus=1

set concealcursor=ni
set conceallevel=2

inoremap <TAB> <C-t>
inoremap <S-TAB> <C-d>

nnoremap <silent>co :set <C-R>=&conceallevel ? 'conceallevel=0' : 'conceallevel=2'<CR><CR>
autocmd FileType markdown set comments=:\*\ | set formatoptions+=ro | set breakindent
nnoremap <silent><esc> :nohlsearch<cr>

set confirm
filetype plugin indent on
set autoindent
set smartindent
set tabstop=4
set shiftwidth=4
set expandtab
set nottimeout

set termguicolors
set background=light

let g:vim_markdown_folding_disabled = 1
let g:vim_markdown_emphasis_multiline = 0
let g:vim_markdown_fenced_languages = ['css', 'javascript', 'js=javascript', 'json=javascript', 'html', 'python', 'cpp', 'bash=sh', 'java']
let g:vim_markdown_math = 1
let g:vim_markdown_strikethrough = 1
let g:vim_markdown_new_list_item_indent = 4
let g:vim_markdown_autowrite = 0
let g:markdown_enable_spell_checking = 0

let g:neosolarized_contrast = 'high'
let g:neosolarized_visibility = 'low'
let g:neosolarized_vertSplitBgTrans = 1
let g:neosolarized_bold = 1
let g:neosolarized_underline = 1
let g:neosolarized_italic = 1
let g:neosolarized_termBoldAsBright = 1

try
    colorscheme NeoSolarized
catch
    message "Can't load NeoSolarized"
endtry

set mouse=a