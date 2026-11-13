# Bonus multi-VM Kubernetes (3 nodes)

## Roles

| IP | Role | Hostname |
|----|------|----------|
| 192.168.56.101 | Control plane | `k8s-cp` |
| 192.168.56.102 | Worker | `k8s-w1` |
| 192.168.56.104 | Worker | `k8s-w2` |

## 0) Copy scripts into each VM

From Windows (PowerShell), if SSH user is `ubuntu` and key/password works:

```powershell
scp -r "d:\University\3\Distributed Systems\Assignments\3\k8s\multi-vm" ubuntu@192.168.56.101:~/
scp -r "d:\University\3\Distributed Systems\Assignments\3\k8s\multi-vm" ubuntu@192.168.56.102:~/
scp -r "d:\University\3\Distributed Systems\Assignments\3\k8s\multi-vm" ubuntu@192.168.56.104:~/
```

## 1) Prepare ALL three VMs

SSH into each and run (Ubuntu):

```bash
# on .101
sudo bash ~/multi-vm/01-prepare-node.sh k8s-cp 192.168.56.101

# on .102
sudo bash ~/multi-vm/01-prepare-node.sh k8s-w1 192.168.56.102

# on .104
sudo bash ~/multi-vm/01-prepare-node.sh k8s-w2 192.168.56.104
```

## 2) Init control plane (ONLY .101)

```bash
sudo bash ~/multi-vm/02-init-control-plane.sh
```

At the end, kubeadm prints a line like:

```bash
kubeadm join 192.168.56.101:6443 --token ... --discovery-token-ca-cert-hash sha256:...
```

Copy that full command.

## 3) Join workers (.102 and .104)

On each worker, as root:

```bash
sudo kubeadm join 192.168.56.101:6443 --token ... --discovery-token-ca-cert-hash sha256:...
```

## 4) Verify (on .101)

```bash
kubectl get nodes -o wide
# expect 3 nodes Ready
```

## 5) Use cluster from Windows (optional)

```powershell
scp ubuntu@192.168.56.101:~/.kube/config $env:USERPROFILE\.kube\config-banking-bonus
$env:KUBECONFIG="$env:USERPROFILE\.kube\config-banking-bonus"
kubectl get nodes -o wide
```

Minikube core stays untouched; switch clusters by changing `KUBECONFIG`.
