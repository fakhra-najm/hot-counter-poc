# DPDK deployment contract

The counter daemon uses the kernel TCP transport until the DPDK target is built and validated. Do not bind the management ENI to DPDK: SSH and control traffic must remain on the primary ENI. Use a dedicated ENA secondary ENI for DPDK and bind only that PCI device to vfio-pci.

The validated host setup must prove IOMMU and vfio availability, DPDK ENA PMD support, RSS queue count matching pinned worker cores, huge-page allocation, and a successful testpmd packet-forwarding check over the dedicated data interface. DPDK is not considered enabled merely because the library is installed.
