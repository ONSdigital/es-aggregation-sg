echo Getting EC2 subnets
aws ec2 describe-subnets --filters Name=tag:Name,Values=results-tf-sub-private-\* --subnet-ids --output json > json_outputs/subnets_output.json
echo Getting EC2 security groups
aws ec2 describe-security-groups --filters Name=tag:Name,Values=results-tf-sg-private --output json > json_outputs/security_groups_output.json