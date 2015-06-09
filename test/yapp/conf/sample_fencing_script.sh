#!/bin/bash

# Print header information.
echo "Begin Fencing Script!"
echo ""

# Count the call parameters.
echo "Count call parameters [${#}]"
echo ""

# Declare an array of delimited parameters.
ARRAY=("${@}")

# Declare a numeric constant of array elements.
ELEMENTS=${#ARRAY[@]}

# Does the parameter account agree with array elements.
if [[ ${#} = ${#ARRAY[@]} ]]; then
  echo "Parameters match exploded array elements."
else
  echo "Parameters [${#}] don't match exploded array elements [${ELEMENTS}]."
fi

# Echo line break.
echo ""

# Echo the parameter list.
for (( i = 0; i < ${ELEMENTS}; i++ )); do
  echo "  ARRAY[${i}]=[${ARRAY[${i}]}]"
done

# Print footer information.
echo ""
echo "End Fencing Script!"
