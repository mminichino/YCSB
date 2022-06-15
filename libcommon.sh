#!/usr/bin/env bash

print_usage() {
if [ -n "$PRINT_USAGE" ]; then
   echo "$PRINT_USAGE"
fi
}

err_exit() {
   if [ -n "$1" ]; then
      echo "[!] Error: $1"
   else
      print_usage
   fi
   exit 1
}

warn_msg() {
   if [ -n "$1" ]; then
      echo "[!] Warning: $1"
   fi
}

info_msg() {
   if [ -n "$1" ]; then
      echo "[i] $1"
   fi
}

get_password() {
   while true
   do
      echo -n "Password: "
      read -s ENTERED_PASSWORD
      echo ""
      echo -n "Retype Password: "
      read -s CHECK_PASSWORD
      echo ""
      if [ "$ENTERED_PASSWORD" != "$CHECK_PASSWORD" ]; then
         echo "Passwords do not match"
      else
         break
      fi
   done
   export PASSWORD=$ENTERED_PASSWORD
}
