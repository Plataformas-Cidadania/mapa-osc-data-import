^p::Pause
^e::ExitApp
^#r::
 MsgBox Rotina Recarregada - Macros Cotidiano
 Reload
return






^#o::


 AbreJanela("IBGE")





return






;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;     Funções     ;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


AbreJanela(my_window_title) {
 Sleep, 300
 SetTitleMatchMode, 2
 WinWait, %my_window_title%
 Loop 3
 {
  IfWinNotActive, %my_window_title%
  {
    WinActivate, %my_window_title%
    WinWaitActive, %my_window_title%, , 10
    Sleep, 100
  }
 }
}

