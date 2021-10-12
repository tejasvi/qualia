if exists("g:loaded_qualia")
    finish
endif
let g:loaded_qualia = 1

function! qualia#install()
    if !has("nvim")
		echoerr "Please install latest nvim. See https://github.com/neovim/neovim/wiki/Installing-Neovim"
    endif
	UpdateRemotePlugins " Install missing packages
	UpdateRemotePlugins
	echomsg "Run nvim .q.md"
endfunction
command! -nargs=0 QualiaInstall call qualia#install()

function! qualia#user_input(prompt)
    call inputsave()
    let l:input = input(a:prompt)
    call inputrestore()
    return l:input
endfunction

function! qualia#search_input_query()
    execute ':SearchQualia '.qualia#user_input("Search: ")
endfunction

fun! s:MoveCursorConcealOffset()
    if &conceallevel == 2
        let [line_number, column_number] = getcurpos()[1:2]
        let line_content = getline('.')
        let conceal_begin_col = match(line_content, '[+*-.] \[\](.\+)  ')
        if conceal_begin_col >= 0 && column_number > conceal_begin_col
            call setpos('.', [0, line_number, column_number + 2, 0])
        endif
    endif
endfun

" https://github.com/jupyterhub/binderhub/issues/741
" https://github.com/ian-r-rose/binder-workspace-demo

if !(exists('g:qualia_no_keymap') && g:qualia_no_keymap)
    nnoremap <LeftMouse> <LeftMouse>:call <SID>MoveCursorConcealOffset()\|echo ''<CR>
    inoremap <LeftMouse> <Esc><LeftMouse>:echo 'Normal Mode'<CR>

    if !exists('g:qualia_prefix_key')
        let g:qualia_prefix_key='<Leader>'
    endif

    function! qualia#set_key_map()
        if !exists('b:qualia_key_map')
            let maplist = ['h :PromoteNode', 'j :ToggleFold', 'K :NavigateNode', 'k :HoistNode', 'l :TransposeNode', 'L :TransposeNode 1', 'p :ToggleQualia', '/ :call qualia#search_input_query()', '? :SearchQualia']
            for i in range(1, 9)
                call add(maplist, i.' :FoldLevel '.i)
            endfor
            call map(maplist, 'g:qualia_prefix_key.v:val')

            call extend(maplist, ['<2-LeftMouse> :ToggleFold', '<3-LeftMouse> :FoldLevel 9', '<RightMouse> <LeftMouse>:TransposeNode'])

            for mapstr in maplist
                execute 'nnoremap <unique><buffer><silent>'.mapstr.'<CR>'
            endfor
            let b:qualia_key_map=1
        endif
    endfunction

    autocmd VimEnter,BufEnter *.q.md call qualia#set_key_map()
endif


function! qualia#pretty_id()
    if !exists('w:matchAdded')
        for [pattern, cchar] in [['\n\s*1[.)]\zs \[](.\{-})\ze  '    , '' ],
                                \['\n\s*\zs[\-*+] \[](.\{-})\ze  '   , '' ],
                                \['\n\s*- \[]([nN].\{-})\zs \ze '    , '•'],
                                \['\n\s*+ \[]([nN].\{-})\zs \ze '    , '‣'],
                                \['\n\s*1[.)] \[]([nN].\{-})\zs \ze ', '│'],
                                \['\n\s*- \[]([tT].\{-})\zs \ze '    , '●'],
                                \['\n\s*+ \[]([tT].\{-})\zs \ze '    , '▶'],
                                \['\n\s*1[.)] \[]([tT].\{-})\zs \ze ', '┃'],
                                \['\n\s*\zs1[.)]\ze \[]([tnTN].\{-})', ' '],
                                \['\n\s*- \[]([NT].\{-}) \zs '       , 'ॱ'],
                                \['\n\s*+ \[]([NT].\{-}) \zs '       , 'ॱ'],
                                \['\n\s*1[.)] \[]([NT].\{-}) \zs '   , 'ॱ']]
            call matchadd('Conceal',pattern, 999, -1, {'conceal':cchar})
        endfor
        let w:matchAdded=1
    endif
endfunction

autocmd VimEnter,WinEnter,BufEnter *.q.md call qualia#pretty_id()

function! FilterQualiaFiles()
    let new_oldfiles = []
    for v_file in v:oldfiles
        if v_file !~ '\.q\.md$'
            call add(new_oldfiles, v_file)
        endif
    endfor
    let v:oldfiles = new_oldfiles
endfunction
autocmd VimEnter,BufNew *.q.md call FilterQualiaFiles()

autocmd VimEnter,WinEnter,TextChanged,FocusGained,BufEnter,InsertLeave,BufFilePost,BufAdd,CursorHold *.q.md TriggerSync 1
autocmd BufEnter *.q.md setlocal filetype=markdown
autocmd VimEnter,WinEnter *.q.md set nofoldenable nowrap
"ॱᐧᣟ⋅⸪⸫⸬⸭⸱ꜗꜘꜙ𑁉𑁊
