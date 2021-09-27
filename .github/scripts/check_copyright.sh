#!/bin/bash

err="0"
printbad() {
  for filename in $1/*.go
  do
    strfound=1
    if grep -Eq "Copyright \(c\) 202[1-9]-present (unTill Pro|Sigma-Soft|Heeus), Ltd." $filename
    then
      strfound=0
    fi
    if grep -Eq "Copyright \(c\) 202[1-9]-present (unTill Pro|Sigma-Soft|Heeus), Ltd., (unTill Pro|Sigma-Soft|Heeus), Ltd." $filename
    then
      strfound=0	
    fi
    if [ $strfound -eq 1 ]
    then
      err="1"
      echo "File: $filename" 
    fi	
  done

  for dirname in $1/* 
  do 
     dir=$dirname
     if [ -d "$dirname" ]
     then
       printbad $dir $cname
     fi
  done
}  

printbad "./."

if [ $err -eq "1" ]
then
    echo "***************************************************************"
    echo "******   File list above has no correct Copyright line   ******"
    echo "***************************************************************"
    exit 1
fi
