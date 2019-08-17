package network

import (
	"bytes"
	"fmt"
	"net"
)

// ExternalIP returns the first outward facing address available.
func ExternalIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return ip.String(), nil
		}
	}
	return "", fmt.Errorf("are you connected to the network?")
}

func ResolveAddress(input string) (string, error) {
	inputParsed, err := net.ResolveTCPAddr("tcp", input)
	if err != nil {
		return "", err
	}
	// If they have specified an IP address to do the things, then prefer that
	// But if there is no IP address then default to our primary external IP.
	if inputParsed.IP != nil && !bytes.Equal(inputParsed.IP, make([]byte, 16)) {
		return inputParsed.String(), nil
	}

	// But if they haven't specified an IP address then we want to assume.
	assumedAddr, err := ExternalIP()
	if err != nil {
		return "", err
	}
	inputParsed, _ = net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", assumedAddr, inputParsed.Port))
	return inputParsed.String(), nil
}
