#!/usr/bin/gawk -f

BEGIN {
   # User input
   # N: number of nodes

   portnumber = 1111
}

END {
   print "["
   for (i = 1; i <= N; i++) {
      if (i < N) {
         COMMA=","
      } else {
         COMMA=""
      }
      printf "\t{\"nodeID\": \"Node%d\", \"url\": \"localhost:%d\"}%s\n",
             i, portnumber + i, COMMA
   }
   print "]"
}
