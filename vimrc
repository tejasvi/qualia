let &runtimepath.=','.escape(expand('<sfile>:p:h'), '\,')
if !has('win32')
    let g:python3_host_prog = substitute(system("which python3.9"), "\n", "", "")
endif
