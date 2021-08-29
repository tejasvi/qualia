if exists("g:loaded_qualia")
    finish
endif
let g:loaded_qualia = 1

function! qualia#trigger_sync() abort
    TriggerSync
endfunction

function! qualia#install()
    if !has("nvim")
		echoerr "Please install latest nvim. See https://github.com/neovim/neovim/wiki/Installing-Neovim"
    endif
	UpdateRemotePlugins " Install missing packages
	UpdateRemotePlugins
	echomsg "Run nvim .q.md"
endfunction
command! -nargs=0 QualiaInstall call qualia#install()

if !(exists('g:qualia_no_keymap') && g:qualia_no_keymap)
    if !exists('g:qualia_prefix_key')
        let g:qualia_prefix_key='<Leader>'
    endif

    function! qualia#user_input(prompt)
        call inputsave()
        let l:input = input(a:prompt)
        call inputrestore()
        return l:input
    endfunction

    function! qualia#search_input_query()
        let l:user_input = qualia#user_input("Search: ")
        if l:user_input !=# ""
            execute ':SearchQualia '.l:user_input
        endif
    endfunction

    function! qualia#set_key_map()
        if !exists('b:qualia_key_map')
            let maplist = ['a :ToggleFold', 'G :NavigateNode', 'g :HoistNode', 't :TransposeNode', 'T :TransposeNode 1', 'p :ToggleParser', '? :SearchQualia', '/ :call qualia#search_input_query()']
            for i in range(1, 9)
                call add(maplist, i.' :FoldLevel '.i)
            endfor
            for mapstr in maplist
                execute 'nnoremap <unique><buffer><silent>'.g:qualia_prefix_key.mapstr.'<CR>'
            endfor
            let b:qualia_key_map=1
        endif
    endfunction

    autocmd VimEnter,BufEnter *.q.md call qualia#set_key_map()
endif


function! qualia#pretty_id()
    if !exists('w:matchAdded')
        for [pattern, cchar] in [['\s*1[.)]\zs [](.\{-})\ze  '         , '' ],
                                \['\s*\zs[\-*+] [](.\{-})\ze  '        , '' ],
                                \['\s*- [](.\{-})\zs \ze '             , '•'],
                                \['\s*+ [](.\{-})\zs \ze '             , '‣'], 
                                \['\s*1[.)] [](.\{-})\zs \ze '         , '┃'], 
                                \['\s*\zs1[.)]\ze [](.\{-})  '         , ' ']]
            call matchadd('Conceal',pattern, 999, -1, {'conceal':cchar})
        endfor
        let w:matchAdded=1
    endif
endfunction
autocmd VimEnter,WinEnter *.q.md call qualia#pretty_id()
