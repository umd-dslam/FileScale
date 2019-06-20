set -xe

## declare an array variable
VOLTDB_PROCEDURES=$(ls | grep java | cut -f 1 -d '.')

cat <<EOF
============================================
Removing the Exist Stored Procedures ...
============================================

EOF


EXISTS_PRCEDURES=$(echo "show procedures" | sqlcmd | sed '1,/^--- User Procedures ------------------------------------------$/d' | awk '{print $1}')

for procedure in $VOLTDB_PROCEDURES
do
    if [[ $EXISTS_PRCEDURES == *"$procedure"* ]];
    then
        echo "DROP PROCEDURE $procedure;" | sqlcmd;
    fi
done

