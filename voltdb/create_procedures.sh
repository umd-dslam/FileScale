## declare an array variable
declare -a VOLTDB_PROCEDURES=("VoltDBStoredProcedureTest" "SetDelegationKeys" "SetStringTable" "RemoveChild" "RemoveBlock" "RemoveAllBlocks" "InsertINode2Block" "InsertXAttrs")

cat <<EOF
============================================
Removing the Exist Stored Procedures ...
============================================

EOF


EXISTS_PRCEDURES=$(echo "show procedures" | sqlcmd | awk '{print $1}')

for procedure in ${VOLTDB_PROCEDURES[@]}
do
    if [[ $EXISTS_PRCEDURES == *"$procedure"* ]];
    then
        echo "DROP PROCEDURE $procedure;" | sqlcmd;
    fi
done

cat <<EOF
============================================
Loading Stored Procedures to VoltDB ...
============================================

EOF


rm -rf *.class
javac *.java
jar cvf storedprocs.jar *.class
echo "LOAD CLASSES storedprocs.jar;" | sqlcmd 

cat <<EOF
============================================
Creating New Stored Procedures ...
============================================

EOF


for procedure in ${VOLTDB_PROCEDURES[@]}
do
    echo "CREATE PROCEDURE FROM CLASS $procedure;" | sqlcmd;
done
