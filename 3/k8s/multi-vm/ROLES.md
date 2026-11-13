# Multi-VM cluster roles

| IP | Hostname suggestion | Role |
|----|---------------------|------|
| `192.168.56.101` | `k8s-cp` | Control plane (master) |
| `192.168.56.102` | `k8s-w1` | Worker 1 |
| `192.168.56.104` | `k8s-w2` | Worker 2 |

Host network: VirtualBox Host-Only (`192.168.56.0/24`).

From Windows, manage the cluster after setup with kubeconfig copied from the control plane.
