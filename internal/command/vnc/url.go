package vnc

import "net/url"

// newVNCURL returns a "vnc://" URL that points to the host
// and carries the credentials, if any.
//
// The credentials are percent-encoded, otherwise characters
// like "@", ":", "/", "?" and "#" in them would corrupt the URL.
func newVNCURL(host string, username string, password string) string {
	vncURL := &url.URL{
		Scheme: "vnc",
		Host:   host,
	}

	if password != "" {
		vncURL.User = url.UserPassword(username, password)
	} else if username != "" {
		vncURL.User = url.User(username)
	}

	return vncURL.String()
}
