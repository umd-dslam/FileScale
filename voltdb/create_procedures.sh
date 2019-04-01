EXISTS_PRCEDURES=$(echo "show procedures" | sqlcmd | awk '{print $1}' | sed -n '/VoltDBStoredProcedureTest/,$p')

for procedure in $EXISTS_PRCEDURES
do
    echo "drop stored procedure: $procedure ... "
    echo "DROP PROCEDURE $procedure" | sqlcmd;
done

for procedure in "VoltDBStoredProcedureTest" "RemoveChild"
do
    echo "create stored procedure: $procedure ... "
    echo "CREATE PROCEDURE FROM CLASS $procedure" | sqlcmd;
done
