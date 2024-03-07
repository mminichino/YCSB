#!/bin/bash
#

function log_output {
    [ -z "$NOLOG" ] && NOLOG=0
    DATE=$(date '+%m-%d-%y_%H:%M:%S')
    [ -z "$LOGFILE" ] && LOGFILE=/var/log/$(basename $0).log
    while read line; do
        [ -z "$line" ] && continue
        if [ "$NOLOG" -eq 0 -a -n "$LOGFILE" ]; then
           echo "$DATE: $line" | tee -a $LOGFILE
        else
           echo "$DATE: $line"
        fi
    done
}

function install_pkg {
  set_linux_type
  case $PKGMGR in
  yum)
    yum install -q -y "$@"
    ;;
  apt)
    apt-get update
    apt-get install -q -y "$@"
    ;;
  *)
    err_exit "Unknown package manager $PKGMGR"
    ;;
  esac
}

function set_linux_type {
  if [ -z "$LINUXTYPE" ]; then
    source /etc/os-release
    export LINUXTYPE=$ID
    case $LINUXTYPE in
    centos)
      PKGMGR="yum"
      SVCMGR="systemctl"
      DEFAULT_ADMIN_GROUP="wheel"
      LINUX_VERSION=$VERSION_ID
      ;;
    rhel)
      PKGMGR="yum"
      SVCMGR="systemctl"
      DEFAULT_ADMIN_GROUP="adm"
      LINUX_VERSION=$(echo $VERSION_ID | cut -d. -f1)
      ;;
    ubuntu)
      PKGMGR="apt"
      SVCMGR="systemctl"
      DEFAULT_ADMIN_GROUP="sudo"
      LINUX_VERSION=$VERSION_CODENAME
      ;;
    *)
      err_exit "Unknown Linux distribution $ID"
      ;;
    esac
  fi
}

function setup_libcouchbase_repo {
set_linux_type
case $LINUXTYPE in
centos|rhel)
  case $LINUX_VERSION in
  7)
cat <<EOF > /etc/yum.repos.d/couchbase.repo
[couchbase]
enabled = 1
name = libcouchbase package for centos7 x86_64
baseurl = https://packages.couchbase.com/clients/c/repos/rpm/el7/x86_64
gpgcheck = 1
gpgkey = https://packages.couchbase.com/clients/c/repos/rpm/couchbase.key
EOF
    ;;
  8)
cat <<EOF > /etc/yum.repos.d/couchbase.repo
[couchbase]
enabled = 1
name = libcouchbase package for centos8 x86_64
baseurl = https://packages.couchbase.com/clients/c/repos/rpm/el8/x86_64
gpgcheck = 1
gpgkey = https://packages.couchbase.com/clients/c/repos/rpm/couchbase.key
EOF
    ;;
  *)
    err_exit "setup_libcouchbase_repo: unsupported linux version: $LINUXTYPE $LINUX_VERSION"
    ;;
  esac
  ;;
ubuntu)
  curl -s -o /var/tmp/couchbase.key https://packages.couchbase.com/clients/c/repos/deb/couchbase.key
  apt-key add /var/tmp/couchbase.key
  case $LINUX_VERSION in
  bionic)
    echo "deb https://packages.couchbase.com/clients/c/repos/deb/ubuntu1804 bionic bionic/main" > /etc/apt/sources.list.d/couchbase.list
    ;;
  focal)
    echo "deb https://packages.couchbase.com/clients/c/repos/deb/ubuntu2004 focal focal/main" > /etc/apt/sources.list.d/couchbase.list
    ;;
  *)
    err_exit "Unsupported Ubuntu version: $LINUX_VERSION"
    ;;
  esac
  ;;
*)
  err_exit "Unknown linux type $LINUXTYPE"
  ;;
esac
}

set_linux_type
echo "System type: $LINUXTYPE" | log_output
setup_libcouchbase_repo
install_pkg libcouchbase3 libcouchbase-devel libcouchbase3-tools
install_pkg jq git maven zip python3
