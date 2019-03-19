package util

import (
	"fmt"
	"github.com/golang/glog"
	"runtime"
)

func Welcome() {
	welcome :=
		`______________________________
\                             \           _         ______ |
 \                             \        /   \___-=O'/|O'/__|
  \       Here we go !!!        \_______\          / | /    )
  /                             /        '/-==__ _/__|/__=-|  -GM
 /                             /         *             \ | |
/                             /                        (o)
------------------------------
`

	glog.Warningf(fmt.Sprintf("\n%s", welcome))
}

func Goodbye() {
	goodbye := `
                ##### | #####
Ohh we crash ? # _ _ #|# _ _ #
               #      |      #
         |       ############
                     # #
  |                  # #
                    #   #
         |     |    #   #      |        |
  |  |             #     #               |
         | |   |   # .-. #         |
                   #( O )#    |    |     |
  |  ################. .###############  |
   ##  _ _|____|     ###     |_ __| _  ##
  #  |                                |  #
  #  |    |    |    |   |    |    |   |  #
   ######################################
                   #     #
                    #####
`

	glog.Warningf(goodbye)
}

func PrintStack() {
	var buf [8192]byte
	n := runtime.Stack(buf[:], false)
	glog.Errorf("==> %s\n", string(buf[:n]))
}