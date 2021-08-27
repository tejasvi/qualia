if exists("g:loaded_qualia")
    finish
endif

function! qualia#install()
    if !has("nvim")
		echoerr "Please install latest nvim. See https://github.com/neovim/neovim/wiki/Installing-Neovim"
    endif
	UpdateRemotePlugins " Install missing packages
	UpdateRemotePlugins
	echomsg "Run nvim .q.md"
endfunction
command! -nargs=0 QualiaInstall call qualia#install()

if !(has('g:qualia_no_keymap') && g:qualia_no_keymap)
    if !has('g:qualia_prefix_key')
        let g:qualia_prefix_key='<Leader>'
    endif

    function! UserInput(prompt)
        call inputsave()
        let l:input = input(a:prompt)
        call inputrestore()
        return l:input
    endfunction

    function! SearchWords()
        let l:user_input = UserInput("Search: ")
        if l:user_input !=# ""
            execute 'SearchQualia '.l:user_input
        endif
    endfunction

    function! QualiaMap()
        let maplist = ['a :ToggleFold', 'G :NavigateNode', 'g :HoistNode', 't :TransposeNode', 'T :TransposeNode 0', 'p :ToggleParser', '? :SearchQualia', '/ :call SearchWords()']
        for i in range(1, 9)
            call add(maplist, i.' :FoldLevel '.i)
        endfor
        for mapstr in maplist
            execute 'nnoremap <buffer><silent>'.g:qualia_prefix_key.mapstr.'<CR>'
        endfor
    endfunction

    autocmd VimEnter,BufEnter *.q.md call QualiaMap()
endif


function! PrettyId()
    if !exists('w:matchAdded')
        for [pattern, cchar] in [['\s*+ [](q://.\{-})\zs \ze '             , '‣'], 
                                \['\s*- [](q://.\{-})\zs \ze '             , '•'], 
                                \['\s*\zs[\-*+] [](q://.\{-})\ze  '        , '' ],
                                \['\s*1[.)] [](q://.\{-})\zs \ze '         , '┃'], 
                                \['\s*\zs1[.)]\ze [](q://.\{-})  '         , ' '], 
                                \['\s*1[.)]\zs [](q://.\{-})\ze  '         , '' ]]
            call matchadd('Conceal',pattern, 999, -1, {'conceal':cchar})
        endfor
        let w:matchAdded=1
    endif
endfunction
autocmd VimEnter,WinEnter *.q.md call PrettyId()

let g:loaded_qualia = 1
