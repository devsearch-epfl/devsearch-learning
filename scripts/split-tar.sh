#!/bin/bash

# Helper script found on the Internet to split tarballs.

# splits a large tar file into a set of smaller ones
#
# Author: Dr. J�rgen Vollmer <juergen.vollmer@informatik-vollmer.de>
# Copyright (C) 2003 Dr. J�rgen Vollmer, Karlsruhe, Germany
# For usage and license agreement, see below (function usage)
#
# Id: split-tar,v 1.30 2010/01/13 17:57:52 vollmer Exp $
# Version: 1.11 of 2006/06/02

#set -x

CMD=`basename $0`
VERSION="1.11"

###############################################################################

usage()
{
    cat <<END
usage: $CMD [options] tarfile.<suffix> (filename|directory)...

  Splits a large tar archive into a set of smaller ones.
  Creates a set of tar archives direct from the files and directories.

  <suffix> is one of tar, tar.gz, tgz, or tar.bz2
  Files are written to tarfile-???.<suffix> into the current working directory,
  where ??? are three digits.
  Note: since a TAR file contains tar-specific administration information
        the resulting tar files may be larger that the specified size.
        For computation only the file size of the sources are used.

  Note: split-tar relies on the GNU version of "tar", "find" and "bash".
  Note: split-tar is not able to read the filenames from stdin.
        Use -T instead.

Options:
  -c       : Create the tar archives from [filename|directory...].
  -C opts  : Pass opts to tar, when creating the tarfile with -c
	     the compression options -z (gzip) or -j (bizp2) are
             added by default, if the <suffix> indicates it.
  -e rate  : To compute the set of files to be put into a compressed
             tarfile, one has to estimate compressed size of each
	     uncompressed source file. To do this a compression program
             indicated by the  tarfile.<suffix> is called (e.g. gzip).
             This may be quite time consiming.

             This overhead my be avoided by giving an "compression rate"
             using the -e option. The real file-size of an an uncompressed
             file is divided by that <rate>. This may result in
             too large or too small result tarfiles. So one has to to some
             trial and error to get the <rate> value right.

             The <rate> is positive number.
  -f prog  : Use prog as "find" program, e.g.
               -f /usr/local/bin/gfind
  -N date  : Only store files newer than <date>.
             Typical format: YYYY-MM-DD or 'YYYY-MM-DD HH:MM:SS' or
             if <date> begins with \`/' or \`.', it is taken to be the name
             of a file whose last-modified time specifies the date.
	     -N passes its argument as tar option \`--newer=<date>'
             (this may be changed in the source of this script, see
              variable TAR_NEWER).
	     -N is valid only if -c is given.
  -h       : Help
  -s sizeK : Maximum size of one tar file in Kilo bytes, default ${DEFAULT_SIZE}
  -s sizeM : Size given in Mega Byte
  -s sizeG : Size given in Giga Byte
  -S       : Split the existing tar archive tarfile.<suffix>
             no [filename|directory...] may be given
             that's the default
  -t prog  : Use prog as "tar" program, e.g.
               -t /usr/local/bin/gtar
  -T file  : Read names to create the archive from <file>
  -v       : Verbose (verbose tar messages)
  -V       : Version.

Example:
  Splitting an already existing archive:
    If foo.tar.gz has a size of 3 M bytes, the command
       split-tar -s 1M foo.tar.gz
    will create the three tar.gz archives:
       foo-000.tar.gz
       foo-001.tar.gz
       foo-002.tar.gz
    which may be unpacked as usual:
       tar -xzvf foo-000.tar.gz
       tar -xzvf foo-001.tar.gz
       tar -xzvf foo-002.tar.gz
    and the the result would be the same as if one unpacks the initial archive
       tar -xzvf foo.tar.gz

  Creating the archives directly from the sources:
       split-tar -e 5 -s 10M -c foo.tar.gz /home/foo
  will create tar archives:
       foo-000.tar.gz, ....  foo-<n>.tar.gz
  containing foo's home directory. A compression rate of 5 is assumed
  for all not already compressed files.

Requirements:
  BASH, GNU-tar, and GNU-find.

Version:
  1.11 of 2006/06/02

Author:
  Dr. J�rgen Vollmer <juergen.vollmer@informatik-vollmer.de>
  If you find this software useful, I would be glad to receive a postcard
  from you, showing the place where you're living.

Homepage:
  http://www.informatik-vollmer.de/software/split-tar.html

Copyright:
  (C) 2003 Dr. J�rgen Vollmer, Viktoriastrasse 15, D-76133 Karlsruhe, Germany

License:
  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; either version 2 of the License, or
  any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
END
exit
}

###############################################################################

DEFAULT_SIZE=1024 # kbyte

# we need the GNU utilities!
# which tar program to use, may be changed with the -t option
TAR=tar

# which find program to use, may be changed with the -t option
FIND=find

# file containing filenames
FILES=${TMP=/tmp}/$CMD.files.$$

# file containing the filename of a single file, too large for a single tar
FILE=${TMP=/tmp}/$CMD.file.$$

# where to untar the source tar file
TAR_DIR=${TMP=/tmp}/$CMD.dir.$$/

# file containing tar sources names
TAR_SOURCES=${TMP=/tmp}/$CMD.tarsources.$$

# remove temporary created files on exit
exit_trap()
{
  if [ $OWN_TAR_SOURCES = NO ]
  then
    rm -fr $TAR_SOURCES
  fi
  rm -fr $FILES $FILE $TAR_DIR
}
trap exit_trap EXIT

# tar-file count
COUNT=0

# flag for selection about OWN_TAR_SOURCES
OWN_TAR_SOURCES=NO

# the GNU-tar option to conserve absolute filenames
# used only if for the -c (create) mode, if the user gives an absolute
# path
# older tar versions may use:
#  TAR_WITH_ABSOLUTE_NAMES=--absolute-paths
TAR_WITH_ABSOLUTE_NAMES=--absolute-names

# the GNU-tar option for storing files newer than DATE
# another possibility would be: --newer-mtime
TAR_NEWER=--newer

# argument of -e
COMPRESSION_RATE=

##############################################################################
# emit an error message and terminate
##############################################################################

error()
{
  echo "$CMD: error $*" 1>&2
  exit 1
}

##############################################################################
# create tar files
##############################################################################

TAR_VERBOSE=
do_tar()
{
  files=$1
  dest=`printf "%s/%s-%03d%s" $DEST_DIR $DEST_BASE $COUNT $SUFFIX`
  touch $dest >/dev/null 2>&1 || error "can not create file $dest"
  if [ $DO_CREATE = NO ]
  then TD="-C $TAR_DIR"
  else TD=
  fi
  $TAR $TD $CREATE_OPTS $TAR_COMPRESS $TAR_VERBOSE \
          -c -f $dest --files-from=$files --no-recursion
  COUNT=$((COUNT + 1))
  ( size=`cat $dest | wc -c`;
    printf "** create: %s: size: %9d (bytes)\n" $dest $size )
}

##############################################################################
# emit all parts of a directory path name
##############################################################################

emit_dir_parts()
{
  local ff="$*"
  while [ ! \( -z "$ff" -o "$ff" = "." -o "$ff" = "/" \) ]
  do
   echo "yyyyy $ff/"
   ff=`dirname "$ff"`
  done
}

##############################################################################
# emit all parts of a directory path name (sorted)
##############################################################################

# LC_ALL=C to get the traditional sort order that uses native byte values.

emit_dir_parts_sorted()
{
  local ff="$*"
  (
    while [ ! \( -z "$ff" -o "$ff" = "." -o "$ff" = "/" \) ]
    do
     echo "$ff"
     ff=`dirname "$ff"`
    done
  ) | (LC_ALL=C sort -u -s)
}

##############################################################################
# check options
##############################################################################

DO_CREATE=NO
CREATE_OPTS=
MAX_SIZE=$((DEFAULT_SIZE * 1024))
TAR_NEWER_ARG=
while getopts cC:e:f:N:hvs:St:T:vV opt "$@"
do
  case $opt in
    c  ) DO_CREATE=YES;;
    C  ) CREATE_OPTS="$CREATE_OPTS $OPTARG";;
    e  ) COMPRESSION_RATE=$OPTARG;;
    f  ) FIND=$OPTARG;;
    N  ) TAR_NEWER_ARG=$OPTARG;;
    S  ) DO_CREATE=NO;;
    s  ) [ x"$OPTARG" = x`expr "$OPTARG" : "\([0-9]*[kKmMgG]\)"` ] ||
	    error "-s expects a number followed by one optional character of KMG"
	 case $OPTARG in
          *[kK] ) MAX_SIZE=$((${OPTARG%[kK]} * 1024));;
          *[mM] ) MAX_SIZE=$((${OPTARG%[mM]} * 1024 * 1024));;
          *[gG] ) MAX_SIZE=$((${OPTARG%[gG]} * 1024 * 1024 * 1024));;
          *     ) MAX_SIZE=$(($OPTARG        * 1024));;
         esac;;
    t)   TAR=$OPTARG;;
    T)   TAR_SOURCES=$OPTARG
         OWN_TAR_SOURCES=YES
         [ -s $TAR_SOURCES ] ||
                 error "-T expects a filename with files to get tar'ed in"
         ;;
    v)   TAR_VERBOSE=-v;;
    V)   echo "$CMD $VERSION"
         exit
         ;;
    h|*) usage;;
  esac
done
shift `expr $OPTIND - 1`

# check correct version of TAR and FIND
if  $TAR --version 2>&1 | grep "GNU tar" > /dev/null
then :
else echo "$CMD: sorry $TAR is no GNU tar"
     exit 1;
fi

if  $FIND --version 2>&1 | grep "GNU find" > /dev/null
then :
else echo "$CMD: sorry $FIND is no GNU find"
     exit 1;
fi

if [ $DO_CREATE == YES ]
then
  if [ $OWN_TAR_SOURCES = YES ]
  then
    [ $# -ge 1 ] || error "expected at least one more argument, for more information: $CMD -h"
    TAR_FILE=$1; shift
  else
    [ $# -ge 2 ] || error "expected at least two arguments, for more information: $CMD -h"
    TAR_FILE=$1; shift
    while [ $# -ge 1 ]
    do
      echo $1 >> $TAR_SOURCES ; shift
      # more $TAR_SOURCES
    done
  fi
  [ -z "$TAR_NEWER_ARG" ] && TAR_NEWER_ARG="1970-01-01 00:00:00"
  TAR_DIR=
else
  [ $# -eq 1 ] || error "expected one argument, for more information: $CMD -h"
  TAR_FILE=$1
  [ -f $TAR_FILE ]        || error "could not read $TAR_FILE"
  [ -z "$TAR_NEWER_ARG" ] || error "-N requires -c"
fi

# COMPRESS_CMD is used only to compute the estimated compressed size fo a file
# it is not used to actually do the compression. That is done via the
# TAR_COMPRESS tar command line option
case `basename $TAR_FILE` in
   *.tar.bz2 ) SUFFIX=".tar.bz2"
	       COMPRESS_CMD="bzip2 --stdout"
               TAR_COMPRESS=--bzip2;;
   *.tar.gz  ) SUFFIX=".tar.gz"
	       COMPRESS_CMD="gzip --stdout --no-name"
	       TAR_COMPRESS=--gzip;;
   *.tgz     ) SUFFIX=".tgz"
	       COMPRESS_CMD="gzip --stdout --no-name"
	       TAR_COMPRESS=--gzip;;
   *.tar     ) SUFFIX=".tar"
	       COMPRESS_CMD=
	       TAR_COMPRESS=;;
   *         ) error "unknown suffix of $TAR_FILE";;
esac
DEST_BASE=`basename $TAR_FILE $SUFFIX`
DEST_DIR=`dirname $TAR_FILE`

##############################################################################
# do the job
##############################################################################

# the size of the files to be tar'ed
cur_size=0

rm -fr $FILES $FILE $TAR_DIR $DEST_BASE-[0-9][0-9][0-9]$SUFFIX

# The line with "xxxx xxxx" indicate: we have seen all files, tar the remaining
# files
# The line with "yyyy <name>" indicate: a directory or other kind of file.
# We have to add directories in order to get the file permissions right.
(
if [ $DO_CREATE = NO ]
then
  ############################################################################
  # unpack the source tar archive
  ############################################################################

  mkdir -p $TAR_DIR || error "can not create $TAR_DIR"
  $TAR -C $TAR_DIR -x $TAR_COMPRESS -f $TAR_FILE || error "can not un-tar $TAR_FILE"

  $FIND $TAR_DIR \( -type f -o -type l \) -a -printf "%s %p\n"
else
  ############################################################################
  # create new archive
  # Note: In order to get file-ownership correct, we have to tar all
  #       all directories and parts of it found in any file-path to be added
  #       in the resulting archive. If we don't do that, we get for
  #       created (intermediate) directories the ownership of the
  #       extractor (e.g.).
  #       Therefore we call tar with the --no-recursion option.
  ############################################################################
  (
     $TAR $TAR_WITH_ABSOLUTE_NAMES       \
          $CREATE_OPTS                   \
          $TAR_NEWER "$TAR_NEWER_ARG"    \
          --files-from=$TAR_SOURCES      \
         -cv -f /dev/null
  ) |
  while read -r f
  do
    if   [ -f "$f" ]
    then wc -c "$f"
	 if [ "${f%/*}" != "$last_dir" ]
	 then last_dir=`dirname "$f"`
              emit_dir_parts "$last_dir"
	 fi
    elif [ -d "$f" ]
    then f=${f%/}
	 emit_dir_parts "$f"
         last_dir="$f"
    else echo "yyyyy $f"
    fi
  done
fi
) | ( LC_ALL=C sort -u -s -k2; echo "xxxx xxxx"; ) | ( sed -e "s|/$||" ) |
while read -r size name
do
   case $size in
    xxx* ) [ -f $FILES ] && do_tar $FILES
	   ;;
    yyy* ) # The file name must be stored too :-)
	   # but it will be compressed too
	   # Add it in any case (ok if we have very bad luck and we're
	   # saving a HUGE directory structure without any files
	   # the resulting archive would be too large).
	   size=$((size + ${#name} / 4))
	   cur_size=$((cur_size + size))
	   echo "$name" | sed -e"s|^$TAR_DIR||" >> $FILES
	   ;;
    *    ) if   [ x"$COMPRESS_CMD" != x ]
	   then # compute estimate of compressed file size
	        case "${name##*.}" in
	          gz | zip | bzip | bzip2 ) ;; # already compressed
	          *  ) if   [ x"$COMPRESSION_RATE" = x ]
		       then size=`$COMPRESS_CMD "$name" | wc -c`
		       else size=$((size / $COMPRESSION_RATE))
		       fi
		       ;;
	        esac
	   fi
	   size=$((size + ${#name} / 4))
		# the file name must be stored too :-)
	        # but it will be compressed too
	   if   [ $size -ge $MAX_SIZE ]
	   then echo "$name" | sed -e"s|^$TAR_DIR||" > $FILE
	        do_tar $FILE
	   elif [ $((size + cur_size)) -ge $MAX_SIZE ]
	   then do_tar $FILES
	        cur_size=$size
		# start new tar archive, so we need to emit all
		# parts of the current files pathname (sorted)
		cat /dev/null > $FILES
		dir_names=$(emit_dir_parts_sorted "`dirname "$name"`")
		if [ -n "$dir_names" ]; then
		    echo "$dir_names" | sed -e"s|^$TAR_DIR||" >> $FILES
		fi
		echo "$name" | sed -e"s|^$TAR_DIR||" >> $FILES
	   else cur_size=$((cur_size + size))
	        echo "$name" | sed -e"s|^$TAR_DIR||" >> $FILES
	   fi
    esac
done

##############################################################################
#                         T h e     E n d
##############################################################################

# Log: split-tar,v $
# Revision 1.30  2010/01/13 17:57:52  vollmer
# typoo
#
# Revision 1.29  2006/07/10 07:17:28  vollmer
# typoo
#
# Revision 1.28  2006/06/02 09:26:07  vollmer
# typoo
#
# Revision 1.27  2006/04/24 14:11:46  vollmer
# typoo
#
# Revision 1.26  2006/02/23 20:01:46  vollmer
# Now all directories get the correct time stamp.
# Sorting works as expected, even if non- 7-bit-ASCII letters are used
# by using LC_ALL=C.
# Thanks to one who wants to be unnamed for sending me the bug-fixes.
#
# Revision 1.25  2005/04/27 13:48:34  vollmer
# Add all intermediate directories of a path explicitly in order to get
# file/directory ownership correctly.
# Thanks to Tom Battisto <tbattist-AT-mailaka.net> for the bug report.
#
# Revision 1.24  2005/04/26 07:56:49  vollmer
# Directory persmissions are set now correctly when unpacking the archives.
# Thanks to Tom Battisto <tbattist-AT-mailaka.net> for the bug report.
#
# Revision 1.23  2005/04/08 20:52:32  vollmer
# Added option -T, thanks to Juergen Kainz <jkainz-AT-transflow.com>
#
# Revision 1.21  2005/04/08 20:14:22  vollmer
# added option -e
#
# Revision 1.20  2004/07/23 21:30:15  vollmer
# - added -f and -t options to specify a FIND and TAR program.
#
# Revision 1.18  2003/11/06 16:24:13  vollmer
# - options passed by -C to tar will be passed now to the do_tar routine
# - \ as part of file names are allowed now
#   Thanks to A. R.
#
# Revision 1.17  2003/11/03 16:42:10  vollmer
# - Added option -N
# - The created tar files are stored now in the given directory and not
#   in the current one.
# Thanks to Martin Walter <martin.walter-AT-erol.at>, who found that bug and
# asked for -N
#
# Revision 1.16  2003/10/31 13:01:51  vollmer
# Creating a splitted tar file from directory works now for absolute
# path names of the directory
#
# Revision 1.15  2003/09/18 17:10:40  vollmer
# Filenames containing blanks are processed correctly if given on the
# command line.
# Thanks to Dr. Jim McCaa <jmccaa-AT-ucar.edu>, who gave me the fix.
#
# Revision 1.14  2003/08/18 07:28:16  vollmer
# The number followed -s must be followed now by k m or g
# (in order to make `expr' more portable)
#
# Revision 1.13  2003/08/12 07:56:38  vollmer
# added an Example
#
# Revision 1.12  2003/08/12 07:22:27  vollmer
# fixed a bug found by Willem Penninckx <willem.penninckx-AT-belgacom.net>:
# filenames may contain now blanks and * and other shell emta charcters.
#
# Revision 1.11  2003/07/29 14:08:10  vollmer
# added the aibility to create the tar archive directly from the sources
# (option -c)
#
# Revision 1.10  2003/07/29 13:01:50  vollmer
# -s accepts size specifier k,K,m,M,g or G
#
# Revision 1.9  2003/07/29 12:38:17  vollmer
# improved computing expected size computation
#
# Revision 1.8  2003/07/21 07:55:32  vollmer
# added --no-name option to the gzip COMPRESS_CMD
#
# Revision 1.7  2003/07/15 08:27:04  vollmer
# - added bzip2, thanks to Martin Deinhofer <martin.deinhofer-AT-gesig.at>
# - added length of file names when computing the size
#
# Revision 1.0  2003/07/02 14:57:17 vollmer
# Initial revision
##############################################################################
