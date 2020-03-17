```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "iam:GetRole",
                "lambda:UpdateFunctionCode",
                "lambda:ListTags",
                "lambda:TagResource",
                "lambda:UntagResource",
                "cloudformation:DescribeStackEvents",
                "cloudformation:UpdateStack",
                "cloudformation:CreateChangeSet",
                "cloudformation:DescribeChangeSet",
                "cloudformation:ExecuteChangeSet",
                "cloudformation:GetTemplateSummary",
                "cloudformation:DescribeStacks",
                "cloudformation:CreateChangeSet"
            ],
            "Resource": [
                
            ]
        }
    ]
}
```
