JDIR=/home/k053370/ExportXML
EXPJAR=$JDIR/target/ExportXML-1.0-SNAPSHOT-jar-with-dependencies.jar
LIB=$JDIR/lib/*

java -cp $EXPJAR:$LIB Main -p conf.prop -t FFI1_AC_LOCKED_EVEN000