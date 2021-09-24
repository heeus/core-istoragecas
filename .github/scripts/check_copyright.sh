#!/bin/bash

err="0"
if [ -f "./.github/scripts/copyright_exception.conf" ]
then
  i=0
  while read line
  do
    if [ ! -z $line ]
    then 
 	  if [ $i -eq 0 ]
	  then 
             excldir=${line}
	  else
             exclcompany=${line}
	  fi 	
    fi	
    i=$((i+1))
    if [ $i -eq 2 ]
    then
	break
    fi
  done < "./.github/scripts/copyright_exception.conf" 
fi

printbad() {
  for filename in $1/*.go
  do
    if ! grep -Fq "Copyright (c) 2021-present ${2}, Ltd." $filename
    then	
      err="1"
      echo "File: $filename"
    fi
  done

  for dirname in $1/* 
  do 
     cname=$2
     dir=$dirname
     if [ -d "$dirname" ]
     then
       if [ $dirname==$excldir ]
       then
          cname=$exclcompany
	  cname="${cname%%[[:cntrl:]]}"
       fi

       printbad $dir $cname
     fi
  done
}  

printbad "./." "Sigma-Soft" 

if [ $err -eq "1" ]
then
    echo "***************************************************************"
    echo "******   File list above has no correct Copyright line   ******"
    echo "***************************************************************"
    exit 1
fi

