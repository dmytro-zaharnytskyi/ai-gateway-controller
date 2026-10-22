.items[]
| select(.metadata.deletionTimestamp == null)
| select(.status.phase == "Running")
| . as $pod
| (($pod.status.containerStatuses // []) | map(select(.name == "manager")) | first // {}) as $manager
| select($manager.ready == true)
| [$pod.metadata.name, $pod.spec.nodeName, $manager.imageID, $manager.image]
| @tsv
