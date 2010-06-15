#!/bin/bash 

cat $X509_CERT_DIR/INDEX.txt | grep Repository | grep software-itb &>/dev/null
if [ $? -eq 0 ]; then
  echo "ITB";
else
  echo "OSG"
fi
indextype=`cat $X509_CERT_DIR/INDEX.txt | grep IndexTypeVersion | awk '{print $3}'`
if [ "$indextype" == "" ]; then
  echo "0"
else
  echo $indextype;
fi
