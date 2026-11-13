#!/usr/bin/env bash
# Run on ALL three VMs (Ubuntu 22.04/24.04 recommended) as root or with sudo.
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: sudo bash 01-prepare-node.sh <hostname> <this-node-ip>"
  echo "Example: sudo bash 01-prepare-node.sh k8s-cp 192.168.56.101"
  exit 1
fi

HOSTNAME="$1"
NODE_IP="$2"

echo "==> hostname + hosts"
hostnamectl set-hostname "$HOSTNAME"
cat >/etc/hosts <<EOF
127.0.0.1 localhost
$NODE_IP $HOSTNAME
192.168.56.101 k8s-cp
192.168.56.102 k8s-w1
192.168.56.104 k8s-w2
EOF

echo "==> disable swap"
swapoff -a
sed -i '/ swap / s/^/#/' /etc/fstab || true

echo "==> kernel modules + sysctl"
cat >/etc/modules-load.d/k8s.conf <<EOF
overlay
br_netfilter
EOF
modprobe overlay
modprobe br_netfilter

cat >/etc/sysctl.d/k8s.conf <<EOF
net.bridge.bridge-nf-call-iptables  = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward                 = 1
EOF
sysctl --system >/dev/null

echo "==> containerd"
apt-get update -y
apt-get install -y apt-transport-https ca-certificates curl gpg containerd

mkdir -p /etc/containerd
containerd config default >/etc/containerd/config.toml
sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml
systemctl restart containerd
systemctl enable containerd

echo "==> kubeadm / kubelet / kubectl"
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.31/deb/Release.key \
  | gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.31/deb/ /' \
  >/etc/apt/sources.list.d/kubernetes.list

apt-get update -y
apt-get install -y kubelet kubeadm kubectl
apt-mark hold kubelet kubeadm kubectl
systemctl enable kubelet

echo "==> done on $HOSTNAME ($NODE_IP)"
kubeadm version
containerd --version
