cat <<EOF
============================================
Removing the Exist Stored Procedures ...
============================================
EOF

EXISTS_PRCEDURES=$(echo "show procedures" | sqlcmd | awk '{print $1}' | sed -n '/VoltDBStoredProcedureTest/,$p')

for procedure in $EXISTS_PRCEDURES
do
    echo "drop stored procedure: $procedure ... "
    echo "DROP PROCEDURE $procedure;" | sqlcmd;
done

cat <<EOF
============================================
Loading Stored Procedures to VoltDB ...
============================================

EOF
rm -rf *.class
javac *.java
jar cvf storedprocs.jar *.class
echo "load classes storedprocs.jar;" | sqlcmd 

cat <<EOF
============================================
Creating New Stored Procedures ...
============================================

EOF

for procedure in "VoltDBStoredProcedureTest" "RemoveChild"
do
    echo "create stored procedure: $procedure ... "
    echo "CREATE PROCEDURE FROM CLASS $procedure;" | sqlcmd;
done
