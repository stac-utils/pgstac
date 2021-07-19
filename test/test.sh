#!/bin/bash
TESTOUTPUT=$(psql -X -f $(dirname $0)/pgtap.sql)
if [ $? != 0 ]; then
    echo "Problem connecting to database to run tests."
    exit 1
fi
echo "$TESTOUTPUT" | grep '^not'
if [ $? == 0 ]; then
    exit 1
else
    echo "All PGTap Tests Passed!"
    exit 0
fi
