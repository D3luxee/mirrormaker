#!/bin/bash
set -e

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}/..

function fail () {
        echo "Aborting due to failure." >&2
        exit 2
}

##pkg build
SNAME=mirrormaker
chmod +x ./mirrormaker
label=$(./mirrormaker --version | awk '{ if(NR==2) print $2 }')
pdir=mi-${SNAME}-${label}

#create basic folder structure
mkdir -p ${pdir}/DEBIAN
mkdir -p ${pdir}/opt/d3luxee/${SNAME}-${label}/bin
cp mirrormaker ${pdir}/opt/d3luxee/${SNAME}-${label}/bin/mirrormaker
description () {
    cat <<EOF
Mirrormaker ${label} 
EOF
}


cat <<EOF > "$pdir"/DEBIAN/control
Package: mi-${SNAME}-${label}
Version: 1.0
Architecture: all
Maintainer: Georg Doser <georg@neuland.tech>
Depends: debconf (>= 0.5.00)
Priority: optional
Description: $( description )
EOF

cat <<EOF > "$pdir"/DEBIAN/postinst
#!/bin/sh

set -e

if [ "\${1}" = "configure" ]; then
  if ! find -P /opt/${SNAME} -type l >/dev/null 2>&1; then
    ln -s 'd3luxee/${SNAME}-${label}/bin' /opt/${SNAME}
  fi
fi
EOF

cat <<EOF > "$pdir"/DEBIAN/postrm
#!/bin/sh

set -e

if [ "\${1}" = "remove" ]; then
  _found="false"

  for dir in /opt/d3luxee/${SNAME}-*; do
    [ -d "\${dir}" ] && _found="true"
    if [ "\${_found:-false}" = "true" ]; then
      break
    fi
  done
  if [ "\${_found:-true}" = "false" -a -L /opt/${SNAME} ]; then
    rm -f /opt/${SNAME} || true
  fi
fi
EOF

chmod a+rx "$pdir"/DEBIAN/*

fakeroot dpkg-deb --build "${pdir}"
