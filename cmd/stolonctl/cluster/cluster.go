package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/gravitational/stolon/cmd/stolonctl/client"
	"github.com/gravitational/stolon/pkg/cluster"
	"github.com/gravitational/trace"
)

func Status(client *client.Client, clusterName string, masterOnly, toJson bool) error {
	clt, err := client.GetCluster(clusterName)
	if err != nil {
		return trace.Wrap(err)
	}

	if masterOnly {
		return masterStatus(clt, toJson)
	}

	tabOut := new(tabwriter.Writer)
	tabOut.Init(os.Stdout, 0, 8, 1, '\t', 0)

	sentinelsInfo, err := clt.GetSentinelsInfo()
	if err != nil {
		return trace.Wrap(err, "cannot get sentinels info")
	}
	lsid, err := clt.GetLeaderSentinelId()
	if err != nil {
		return trace.Wrap(errors.New("cannot get leader sentinel info"))
	}

	fmt.Println("Active sentinels")
	if len(sentinelsInfo) == 0 {
		fmt.Println("No active sentinels")
	} else {
		sort.Sort(sentinelsInfo)
		fmt.Fprintf(tabOut, "ID\tLISTENADDRESS\tLEADER\n")
		for _, si := range sentinelsInfo {
			leader := false
			if lsid != "" {
				if si.ID == lsid {
					leader = true
				}
			}
			fmt.Fprintf(tabOut, "%s\t%s:%s\t%t\n", si.ID, si.ListenAddress, si.Port, leader)
			tabOut.Flush()
		}
	}

	proxiesInfo, err := clt.GetProxiesInfo()
	if err != nil {
		return trace.Wrap(err, "cannot get proxies info")
	}

	fmt.Println("Active proxies")
	if len(proxiesInfo) == 0 {
		fmt.Println("No active proxies")
	} else {
		sort.Sort(proxiesInfo)
		fmt.Fprintf(tabOut, "ID\tLISTENADDRESS\tCV VERSION\n")
		for _, pi := range proxiesInfo {
			fmt.Fprintf(tabOut, "%s\t%s:%s\t%d\n", pi.ID, pi.ListenAddress, pi.Port, pi.ClusterViewVersion)
			tabOut.Flush()
		}
	}

	clusterData, _, err := clt.GetClusterData()
	if err != nil {
		return trace.Wrap(err, "cannot get cluster data")
	}
	if clusterData == nil {
		return trace.Wrap(err, "cluster data not available")
	}
	cv := clusterData.ClusterView
	kss := clusterData.KeepersState

	fmt.Println("Keepers")
	if kss == nil {
		fmt.Println("No keepers state available")
	} else {
		kssKeys := kss.SortedKeys()
		fmt.Fprintf(tabOut, "ID\tLISTENADDRESS\tPG LISTENADDRESS\tCV VERSION\tHEALTHY\n")
		for _, k := range kssKeys {
			ks := kss[k]
			fmt.Fprintf(tabOut, "%s\t%s:%s\t%s:%s\t%d\t%t\n", ks.ID, ks.ListenAddress, ks.Port, ks.PGListenAddress, ks.PGPort, ks.ClusterViewVersion, ks.Healthy)
		}
	}
	tabOut.Flush()

	fmt.Println("Required Cluster View")
	fmt.Printf("Version: %d", cv.Version)
	if cv == nil {
		fmt.Println("No clusterview available")
	} else {
		fmt.Printf("Master: %s\n", cv.Master)
		fmt.Println("Keepers tree")
		for _, mr := range cv.KeepersRole {
			if mr.Follow == "" {
				printTree(mr.ID, cv, 0, "", true)
			}
		}
	}

	fmt.Println("")
	return nil
}

func masterStatus(clt *client.ClusterClient, toJson bool) error {
	clusterData, _, err := clt.GetClusterData()
	if err != nil {
		return trace.Wrap(err, "cannot get cluster data")
	}
	if clusterData == nil {
		return trace.NotFound("cluster data not available")
	}
	cv := clusterData.ClusterView
	kss := clusterData.KeepersState
	masterData := kss[cv.Master]
	if toJson {
		data, err := json.Marshal(masterData)
		if err != nil {
			return trace.Wrap(err, "can't convert to json")
		}
		fmt.Println(string(data))
	} else {
		fmt.Println(masterData)
	}

	return nil
}

func PrintConfig(clt *client.Client, clusterName string) error {
	cluster, err := clt.GetCluster(clusterName)
	if err != nil {
		return trace.Wrap(err)
	}
	cfg, err := cluster.Config()
	if err != nil {
		return trace.Wrap(err)
	}
	data, err := json.MarshalIndent(cfg, "", "\t")
	if err != nil {
		return trace.Wrap(err, "failed to marshal configuration")
	}
	fmt.Fprintln(os.Stdout, data)

	return nil
}

func PatchConfig(clt *client.Client, clusterName string, patchFile string, readStdin bool) error {
	data, err := readFile(patchFile, readStdin)
	if err != nil {
		return trace.Wrap(err)
	}
	cluster, err := clt.GetCluster(clusterName)
	if err != nil {
		return trace.Wrap(err)
	}
	err = cluster.PatchConfig(data)

	return trace.Wrap(err)
}

func ReplaceConfig(clt *client.Client, clusterName string, replaceFile string, readStdin bool) error {
	data, err := readFile(replaceFile, readStdin)
	if err != nil {
		return trace.Wrap(err)
	}
	cluster, err := clt.GetCluster(clusterName)
	if err != nil {
		return trace.Wrap(err)
	}
	err = cluster.ReplaceConfig(data)

	return trace.Wrap(err)
}

func List(clt *client.Client) error {
	clusters, err := clt.Clusters()
	if err != nil {
		return trace.Wrap(err)
	}
	for _, cluster := range clusters {
		fmt.Fprintln(os.Stdout, cluster)
	}

	return nil
}

func readFile(fileName string, readStdin bool) ([]byte, error) {
	if (readStdin && fileName != "") || (!readStdin && fileName == "") {
		return nil, trace.BadParameter("need either file to read from or readStdin option")
	}
	var config []byte
	var err error
	if readStdin {
		config, err = ioutil.ReadAll(os.Stdin)
		if err != nil {
			return nil, trace.Wrap(err, "cannot read config file from stdin")
		}
	} else {
		config, err = ioutil.ReadFile(fileName)
		if err != nil {
			return nil, trace.Wrap(err, "can not read file")
		}
	}

	return config, trace.Wrap(err)
}

func printTree(id string, cv *cluster.ClusterView, level int, prefix string, tail bool) {
	out := prefix
	if level > 0 {
		if tail {
			out += "└─"
		} else {
			out += "├─"
		}
	}
	out += id
	if id == cv.Master {
		out += " (master)"
	}
	fmt.Println(out)
	followersIDs := cv.GetFollowersIDs(id)
	c := len(followersIDs)
	for i, f := range cv.GetFollowersIDs(id) {
		emptyspace := ""
		if level > 0 {
			emptyspace = "  "
		}
		linespace := "│ "
		if i < c-1 {
			if tail {
				printTree(f, cv, level+1, prefix+emptyspace, false)
			} else {
				printTree(f, cv, level+1, prefix+linespace, false)
			}
		} else {
			if tail {
				printTree(f, cv, level+1, prefix+emptyspace, true)
			} else {
				printTree(f, cv, level+1, prefix+linespace, true)
			}
		}
	}
}
