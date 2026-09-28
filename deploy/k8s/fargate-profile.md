# EKS Fargate profile for canoe workers

The coordinator itself runs on regular EKS compute (e.g. a small
system node group), but every pod it creates for Python step workers and
Spark launcher/driver/executor pods must land on Fargate so the "quorum"
of workers scales without managing EC2 capacity.

Create a Fargate profile selecting the `canoe-workers` namespace:

```
eksctl create fargateprofile \
  --cluster <cluster-name> \
  --name canoe-workers \
  --namespace canoe-workers
```

Or via the AWS CLI / Terraform, an `aws_eks_fargate_profile` resource with:

```hcl
resource "aws_eks_fargate_profile" "canoe_workers" {
  cluster_name           = var.cluster_name
  fargate_profile_name   = "canoe-workers"
  pod_execution_role_arn = aws_iam_role.canoe_fargate_pod_execution.arn
  subnet_ids              = var.private_subnet_ids

  selector {
    namespace = "canoe-workers"
  }
}
```

Notes / Fargate constraints that shaped the coordinator's pod specs:

- No DaemonSets, no `hostPort`, no privileged containers — the step
  worker pods (plain `Pod`s, not a Deployment — see README.md's "Step
  pods, not Deployments") and the spark-submit Deployment in this repo
  avoid all three.
- Every pod needs its own IAM permissions via IRSA (`serviceAccountName` +
  an annotated ServiceAccount), not node-level instance profiles — see
  `rbac.yaml` and `service-accounts.yaml` for the roles the coordinator,
  Python workers (S3 code fetch), and Spark pods (S3 datalake writes) need.
- Fargate pods take longer to schedule than pods on warm EC2 capacity
  (new Fargate capacity is provisioned per pod). `ScalingConfig.cooldown_secs`
  in the pipeline config exists partly to avoid thrashing while pods are
  still coming up.
