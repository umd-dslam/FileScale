set -xe

## declare an array variable
declare -a VOLTDB_PROCEDURES=("VoltDBStoredProcedureTest" "SetPersistTokens" "SetDelegationKeys" "SetStringTable" "RemoveChild" "RemoveBlock" "RemoveAllBlocks" "InsertINode2Block" "InsertXAttrs")

cat <<EOF
============================================
Removing the Exist Stored Procedures ...
============================================

EOF


EXISTS_PRCEDURES=$(echo "show procedures" | sqlcmd | sed '1,/^--- User Procedures ------------------------------------------$/d' | awk '{print $1}')

for procedure in ${VOLTDB_PROCEDURES[@]}
do
    if [[ $EXISTS_PRCEDURES == *"$procedure"* ]];
    then
        echo "DROP PROCEDURE $procedure;" | sqlcmd;
    fi
done

