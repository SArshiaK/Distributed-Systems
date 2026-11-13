#!/usr/bin/env bash
# Run ONLY on 192.168.56.101 (k8s-cp) after 01-prepare-node.sh
set -euo pipefail

CP_IP="192.168.56.101"
POD_CIDR="10.244.0.0/16"

echo "==> kubeadm init"
kubeadm init \
  --apiserver-advertise-address="$CP_IP" \
  --pod-network-cidr="$POD_CIDR" \
  --upload-certs

echo "==> kubeconfig for ubuntu user (or current non-root)"
TARGET_USER="${SUDO_USER:-$USER}"
TARGET_HOME=$(eval echo "~$TARGET_USER")
mkdir -p "$TARGET_HOME/.kube"
cp -f /etc/kubernetes/admin.conf "$TARGET_HOME/.kube/config"
chown -R "$TARGET_USER:$TARGET_USER" "$TARGET_HOME/.kube"

echo "==> install Flannel CNI"
sudo -u "$TARGET_USER" kubectl apply -f https://github.com/flannel-io/flannel/releases/latest/download/kube-flannel.yml

echo ""
echo "==> SAVE the join command printed above by kubeadm, or regenerate with:"
echo "    kubeadm token create --print-join-command"
echo ""
echo "==> After workers join, check:"
echo "    kubectl get nodes -o wide"
