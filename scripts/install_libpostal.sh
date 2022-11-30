#!/bin/bash

install_linux() {
  sudo apt-get install curl autoconf automake libtool pkg-config
  install_nix "$1" "$2"

}

install_osx() {
  brew install curl autoconf automake libtool pkg-config
  install_nix "$1" "$2"
}

install_nix() {
  (cd "$1"

  git clone https://github.com/openvenues/libpostal
  cd libpostal
  ./bootstrap.sh
  ./configure --datadir="$2"
  make -j4
  sudo make install)

  # On Linux it's probably a good idea to run
  sudo ldconfig
}

install_windows() {
  pacman -Syu

  pacman -S autoconf automake curl git make libtool gcc mingw-w64-x86_64-gcc

  (cd "$1"

  git clone https://github.com/openvenues/libpostal
  cd libpostal
  cp -rf windows/* ./
  ./bootstrap.sh
  ./configure --datadir="$2"
  make -j4
  make install)
}

if [ $# -lt 2 ]; then
  echo "Usage: $0 [install_dir] [data_dir]"
  exit 1
fi

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        install_linux "$1" "$2"
elif [[ "$OSTYPE" == "darwin"* ]]; then
        install_osx "$1" "$2"
elif [[ "$OSTYPE" == "msys" ]]; then
        install_windows "$1" "$2"
else
        echo "ERROR: Don't know how to install libpostal on $OSTYPE"
        exit 1
fi
