which python
python --version

which python3
python3 --version

date
cdkDeployStatus=$?
echo "cdkDeployStatus " $cdkDeployStatus

if [ $cdkDeployStatus -eq 0 ]
then
    echo "in if"
else
    echo "in else"
fi
#[ $status -eq 0 ] && echo "$cmd command was successful" || echo "$cmd failed"

