nnoremap <SPACE> <Nop>
let mapleader = ' '

let g:plug_install = 0
let autoload_plug_path = stdpath('data') . '/site/autoload/plug.vim'
if !filereadable(autoload_plug_path)
    silent exe '!curl -fL --create-dirs -o ' . autoload_plug_path .  ' https://raw.github.com/junegunn/vim-plug/master/plug.vim'
    execute 'source ' . fnameescape(autoload_plug_path)
    let g:plug_install = 1
endif

call plug#begin(stdpath('data') . '/plugged')
    Plug 'overcache/NeoSolarized'
    Plug 'tejasvi/vim-markdown'
    Plug 'junegunn/fzf', { 'do': { -> fzf#install() } }
    Plug 'tejasvi/ConcealImproved'
    Plug 'tejasvi8874/qualia', { 'do': ':QualiaInstall', 'branch': 'dev'}
call plug#end()

if g:plug_install
    PlugInstall --sync | quit
else
    autocmd VimEnter *
      \  if len(filter(values(g:plugs), '!isdirectory(v:val.dir)'))
      \|   PlugInstall --sync | q
      \| endif
endif

autocmd CursorHoldI *.q.md TriggerSync 1

let g:vim_markdown_folding_disabled = 1
let g:vim_markdown_emphasis_multiline = 0
let g:vim_markdown_fenced_languages = ['css', 'javascript', 'js=javascript', 'json=javascript', 'html', 'python', 'cpp', 'bash=sh', 'java']
let g:vim_markdown_math = 1
let g:vim_markdown_strikethrough = 1
let g:vim_markdown_new_list_item_indent = 4
let g:vim_markdown_autowrite = 0
let g:markdown_enable_spell_checking = 0

let g:neosolarized_contrast = 'high'
let g:neosolarized_visibility = 'high'
let g:neosolarized_vertSplitBgTrans = 1
let g:neosolarized_bold = 1
let g:neosolarized_underline = 1
let g:neosolarized_italic = 1
let g:neosolarized_termBoldAsBright = 1
colorscheme NeoSolarized
set laststatus=1
autocmd BufWinEnter,WinEnter term://* startinsert

set termguicolors
set background=light
set concealcursor=ni
set conceallevel=2

set inccommand=split
inoremap <TAB> <C-t>
inoremap <S-TAB> <C-d>

function! Fold()
    let line = getline(v:foldstart)
    line = substitute(line, '\s*\zs\d*[.\-*+] [](q://.\{-}) \ze ', '•', '')
    let plusnum = v:foldend - v:foldstart
    let offset = plusnum < 10 ? 2 : plusnum < 100 ? 3 : 4
    return line . repeat(' ', winwidth(0) - strdisplaywidth(line) - offset) . '+' . plusnum
endfunction
set foldtext=Fold()

set fillchars=eob:ॱ
autocmd BufEnter,FocusGained,CursorHold,FileWritePre,BufWritePre * checktime "reload file
autocmd BufEnter * let &ruler=&filetype==#'markdown' ? 0 : 1 | let &showmode=&ruler
autocmd CmdlineLeave *.md : echo ''
nnoremap <silent>co :set <C-R>=&conceallevel ? 'conceallevel=0' : 'conceallevel=2'<CR><CR>

set mouse=a
set mousemodel=popup_setpos

filetype plugin indent on
set autoindent
set smartindent
autocmd FileType markdown set comments=:\*\ | set formatoptions+=ro | set breakindent | set linebreak

nnoremap Y y$
nnoremap <silent><esc> :nohlsearch<cr>
nnoremap <CR> :
set iskeyword+=-
xnoremap <expr> :s mode()==#'V' ? ':s' : ':s/\%V\%V/<Left><Left><Left><Left>'
set tabstop=4
set shiftwidth=4
set expandtab
set incsearch
set ignorecase
set smartcase
set hlsearch
set directory^=$HOME/.vimfiles// | set backupdir^=$HOME/.vimfiles// | set undodir^=$HOME/.vimfiles// | set backup | set undofile |-
set nottimeout
" set ttimeoutlen=0
set foldmethod=indent
set confirm
set autochdir
set nohidden

" ─━│┃┄┅┆┇┈┉┊┋┌┍┎┏┐┑┒┓└┕┖┗┘┙┚┛├┝┞┟┠┡┢┣┤┥┦┧┨┩┪┫┬┭┮┯┰┱┲┳┴┵┶┷┸┹┺┻┼┽┾┿╀╁╂╃╄╅╆╇╈╉╊╋╌╍╎╏═║╒╓╔╕╖╗╘╙╚╛╜╝╞╟╠╡╢╣╤╥╦╧╨╩╪╫╬╭╮╯╰╱╲╳╴╵╶╷╸╹╺╻╼╽╾╿
